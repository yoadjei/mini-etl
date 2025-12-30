"""
Retry utilities with exponential backoff using tenacity.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Tuple, Type
import logging
from functools import wraps

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    RetryError,
)

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    
    max_attempts: int = 3
    min_wait_seconds: float = 1.0
    max_wait_seconds: float = 60.0
    exponential_base: float = 2.0
    retry_exceptions: Tuple[Type[Exception], ...] = field(
        default_factory=lambda: (ConnectionError, TimeoutError, OSError)
    )
    
    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.min_wait_seconds < 0:
            raise ValueError("min_wait_seconds must be non-negative")
        if self.max_wait_seconds < self.min_wait_seconds:
            raise ValueError("max_wait_seconds must be >= min_wait_seconds")


def retry_with_backoff(
    config: Optional[RetryConfig] = None,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable:
    """
    Decorator factory for retrying functions with exponential backoff.
    
    Args:
        config: RetryConfig instance. Uses defaults if None.
        on_retry: Optional callback called before each retry with (exception, attempt_number).
    
    Returns:
        Decorator function.
    
    Example:
        @retry_with_backoff(RetryConfig(max_attempts=5))
        def fetch_data():
            ...
    """
    if config is None:
        config = RetryConfig()
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            last_exception: Optional[Exception] = None
            
            # Build tenacity retry decorator
            tenacity_retry = retry(
                stop=stop_after_attempt(config.max_attempts),
                wait=wait_exponential(
                    multiplier=config.min_wait_seconds,
                    max=config.max_wait_seconds,
                    exp_base=config.exponential_base,
                ),
                retry=retry_if_exception_type(config.retry_exceptions),
                before_sleep=before_sleep_log(logger, logging.WARNING),
                reraise=True,
            )
            
            @tenacity_retry
            def inner():
                nonlocal attempt, last_exception
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except config.retry_exceptions as e:
                    last_exception = e
                    if on_retry and attempt < config.max_attempts:
                        on_retry(e, attempt)
                    raise
            
            try:
                return inner()
            except RetryError:
                if last_exception:
                    raise last_exception
                raise
        
        return wrapper
    return decorator


class RetryableOperation:
    """
    Context manager for retrying a block of code.
    
    Example:
        with RetryableOperation(RetryConfig(max_attempts=3)) as op:
            while op.should_retry():
                try:
                    result = risky_operation()
                    op.success()
                except ConnectionError as e:
                    op.failed(e)
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self._attempt = 0
        self._succeeded = False
        self._last_exception: Optional[Exception] = None
    
    def __enter__(self) -> "RetryableOperation":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._succeeded and self._last_exception:
            raise self._last_exception
        return False
    
    def should_retry(self) -> bool:
        """Check if another retry attempt should be made."""
        if self._succeeded:
            return False
        return self._attempt < self.config.max_attempts
    
    def success(self) -> None:
        """Mark the operation as successful."""
        self._succeeded = True
    
    def failed(self, exception: Exception) -> None:
        """Record a failure and prepare for retry."""
        self._attempt += 1
        self._last_exception = exception
        
        if self._attempt >= self.config.max_attempts:
            logger.error(
                f"Operation failed after {self._attempt} attempts: {exception}"
            )
        else:
            wait_time = min(
                self.config.min_wait_seconds * (self.config.exponential_base ** (self._attempt - 1)),
                self.config.max_wait_seconds,
            )
            logger.warning(
                f"Attempt {self._attempt} failed: {exception}. "
                f"Retrying in {wait_time:.1f}s..."
            )
            import time
            time.sleep(wait_time)
