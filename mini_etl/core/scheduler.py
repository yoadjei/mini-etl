"""
Simple scheduling utilities for running pipelines on a schedule.
"""

import threading
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
import re

logger = logging.getLogger(__name__)


@dataclass
class ScheduleEntry:
    """A scheduled pipeline execution."""
    
    name: str
    schedule: str  # Cron-like expression or interval
    pipeline_func: Callable[[], Any]
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    enabled: bool = True
    run_count: int = 0
    last_error: Optional[str] = None


class CronParser:
    """
    Simple cron expression parser.
    
    Supports: minute hour day_of_month month day_of_week
    Special values: * (any), */N (every N), N (specific)
    
    Examples:
        "0 * * * *"     - Every hour at minute 0
        "*/15 * * * *"  - Every 15 minutes
        "0 0 * * *"     - Daily at midnight
        "0 0 * * 0"     - Weekly on Sunday at midnight
    """
    
    def __init__(self, expression: str):
        self.expression = expression
        self._parse(expression)
    
    def _parse(self, expression: str) -> None:
        """Parse cron expression into components."""
        parts = expression.strip().split()
        
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression: expected 5 parts, got {len(parts)}"
            )
        
        self.minute = self._parse_field(parts[0], 0, 59)
        self.hour = self._parse_field(parts[1], 0, 23)
        self.day = self._parse_field(parts[2], 1, 31)
        self.month = self._parse_field(parts[3], 1, 12)
        self.weekday = self._parse_field(parts[4], 0, 6)
    
    def _parse_field(self, field: str, min_val: int, max_val: int) -> List[int]:
        """Parse a single cron field."""
        if field == "*":
            return list(range(min_val, max_val + 1))
        
        if field.startswith("*/"):
            step = int(field[2:])
            return list(range(min_val, max_val + 1, step))
        
        if "," in field:
            return [int(v) for v in field.split(",")]
        
        if "-" in field:
            start, end = field.split("-")
            return list(range(int(start), int(end) + 1))
        
        return [int(field)]
    
    def matches(self, dt: datetime) -> bool:
        """Check if datetime matches the cron expression."""
        return (
            dt.minute in self.minute
            and dt.hour in self.hour
            and dt.day in self.day
            and dt.month in self.month
            and dt.weekday() in self.weekday
        )
    
    def next_occurrence(self, after: Optional[datetime] = None) -> datetime:
        """Calculate the next occurrence after the given time."""
        if after is None:
            after = datetime.now()
        
        # Start from the next minute
        candidate = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        
        # Search up to 1 year ahead
        max_iterations = 525600  # minutes in a year
        
        for _ in range(max_iterations):
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        
        raise ValueError("Could not find next occurrence within 1 year")


class IntervalParser:
    """
    Parse simple interval expressions.
    
    Examples:
        "30s"   - Every 30 seconds
        "5m"    - Every 5 minutes
        "2h"    - Every 2 hours
        "1d"    - Every day
    """
    
    PATTERN = re.compile(r"^(\d+)\s*(s|sec|m|min|h|hr|hour|d|day)s?$", re.IGNORECASE)
    
    UNITS = {
        "s": 1, "sec": 1,
        "m": 60, "min": 60,
        "h": 3600, "hr": 3600, "hour": 3600,
        "d": 86400, "day": 86400,
    }
    
    def __init__(self, expression: str):
        self.expression = expression
        self.seconds = self._parse(expression)
    
    def _parse(self, expression: str) -> int:
        """Parse interval to seconds."""
        match = self.PATTERN.match(expression.strip())
        if not match:
            raise ValueError(f"Invalid interval expression: {expression}")
        
        value = int(match.group(1))
        unit = match.group(2).lower()
        
        return value * self.UNITS[unit]
    
    def next_occurrence(self, after: Optional[datetime] = None) -> datetime:
        """Calculate next occurrence."""
        if after is None:
            after = datetime.now()
        return after + timedelta(seconds=self.seconds)


class Scheduler:
    """
    Simple scheduler for running pipelines on a schedule.
    
    Example:
        scheduler = Scheduler()
        
        @scheduler.schedule("*/5 * * * *")  # Every 5 minutes
        def my_pipeline():
            pipeline.run()
        
        # Or manually
        scheduler.add("hourly_job", "0 * * * *", pipeline.run)
        
        # Start scheduler in background
        scheduler.start()
        
        # ... later
        scheduler.stop()
    """
    
    def __init__(self):
        self._entries: Dict[str, ScheduleEntry] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
    
    def add(
        self,
        name: str,
        schedule: str,
        func: Callable[[], Any],
        enabled: bool = True,
    ) -> None:
        """
        Add a scheduled job.
        
        Args:
            name: Unique job name.
            schedule: Cron expression or interval (e.g., "30m").
            func: Function to execute.
            enabled: Whether job is enabled.
        """
        entry = ScheduleEntry(
            name=name,
            schedule=schedule,
            pipeline_func=func,
            enabled=enabled,
        )
        
        # Calculate next run
        entry.next_run = self._calculate_next_run(schedule)
        self._entries[name] = entry
        
        logger.info(f"Scheduled job '{name}': next run at {entry.next_run}")
    
    def remove(self, name: str) -> None:
        """Remove a scheduled job."""
        if name in self._entries:
            del self._entries[name]
            logger.info(f"Removed job '{name}'")
    
    def enable(self, name: str) -> None:
        """Enable a job."""
        if name in self._entries:
            self._entries[name].enabled = True
    
    def disable(self, name: str) -> None:
        """Disable a job."""
        if name in self._entries:
            self._entries[name].enabled = False
    
    def schedule(self, schedule_expr: str):
        """
        Decorator to schedule a function.
        
        Example:
            @scheduler.schedule("0 * * * *")
            def hourly_job():
                ...
        """
        def decorator(func: Callable) -> Callable:
            self.add(func.__name__, schedule_expr, func)
            return func
        return decorator
    
    def _calculate_next_run(
        self, 
        schedule: str,
        after: Optional[datetime] = None,
    ) -> datetime:
        """Calculate next run time from schedule expression."""
        # Try cron first
        try:
            cron = CronParser(schedule)
            return cron.next_occurrence(after)
        except ValueError:
            pass
        
        # Try interval
        try:
            interval = IntervalParser(schedule)
            return interval.next_occurrence(after)
        except ValueError:
            pass
        
        raise ValueError(f"Could not parse schedule: {schedule}")
    
    def start(self, blocking: bool = False) -> None:
        """
        Start the scheduler.
        
        Args:
            blocking: If True, run in foreground. If False, run in background thread.
        """
        if self._running:
            logger.warning("Scheduler is already running")
            return
        
        self._running = True
        self._stop_event.clear()
        
        if blocking:
            self._run_loop()
        else:
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
            logger.info("Scheduler started in background")
    
    def stop(self, timeout: float = 5.0) -> None:
        """Stop the scheduler."""
        if not self._running:
            return
        
        logger.info("Stopping scheduler...")
        self._stop_event.set()
        self._running = False
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        
        logger.info("Scheduler stopped")
    
    def _run_loop(self) -> None:
        """Main scheduler loop."""
        logger.info("Scheduler loop started")
        
        while not self._stop_event.is_set():
            now = datetime.now()
            
            for entry in self._entries.values():
                if not entry.enabled:
                    continue
                
                if entry.next_run and now >= entry.next_run:
                    self._execute_job(entry)
                    entry.next_run = self._calculate_next_run(
                        entry.schedule, after=now
                    )
            
            # Sleep briefly to avoid busy waiting
            self._stop_event.wait(timeout=1.0)
    
    def _execute_job(self, entry: ScheduleEntry) -> None:
        """Execute a scheduled job."""
        logger.info(f"Executing scheduled job: {entry.name}")
        entry.last_run = datetime.now()
        entry.run_count += 1
        
        try:
            entry.pipeline_func()
            entry.last_error = None
            logger.info(f"Job '{entry.name}' completed successfully")
        except Exception as e:
            entry.last_error = str(e)
            logger.error(f"Job '{entry.name}' failed: {e}")
    
    def status(self) -> List[Dict[str, Any]]:
        """Get status of all scheduled jobs."""
        return [
            {
                "name": entry.name,
                "schedule": entry.schedule,
                "enabled": entry.enabled,
                "last_run": entry.last_run.isoformat() if entry.last_run else None,
                "next_run": entry.next_run.isoformat() if entry.next_run else None,
                "run_count": entry.run_count,
                "last_error": entry.last_error,
            }
            for entry in self._entries.values()
        ]
    
    def run_now(self, name: str) -> None:
        """Immediately execute a job by name."""
        if name not in self._entries:
            raise ValueError(f"Job '{name}' not found")
        
        self._execute_job(self._entries[name])
