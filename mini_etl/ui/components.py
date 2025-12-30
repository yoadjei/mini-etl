"""
Reusable UI components for the mini-etl dashboard.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime
import streamlit as st


def render_metric_card(
    label: str,
    value: Any,
    delta: Optional[str] = None,
    delta_color: str = "normal",
) -> None:
    """Render a styled metric card."""
    delta_html = ""
    if delta:
        color = {
            "normal": "#10b981",
            "inverse": "#ef4444",
            "off": "#6b7280",
        }.get(delta_color, "#10b981")
        delta_html = f'<div style="color: {color}; font-size: 0.8rem; margin-top: 0.25rem;">{delta}</div>'
    
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{value}</div>
            <div class="metric-label">{label}</div>
            {delta_html}
        </div>
    """, unsafe_allow_html=True)


def render_status_badge(status: str) -> str:
    """Return HTML for a status badge."""
    status_class = {
        "running": "status-running",
        "success": "status-success",
        "completed": "status-success",
        "error": "status-error",
        "failed": "status-error",
        "idle": "status-idle",
        "pending": "status-idle",
    }.get(status.lower(), "status-idle")
    
    return f'<span class="{status_class}">{status}</span>'


def render_pipeline_card(
    name: str,
    description: str = "",
    status: str = "idle",
    last_run: Optional[datetime] = None,
    row_count: int = 0,
) -> None:
    """Render a pipeline summary card."""
    last_run_str = last_run.strftime("%Y-%m-%d %H:%M") if last_run else "Never"
    status_badge = render_status_badge(status)
    
    st.markdown(f"""
        <div class="pipeline-card">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div class="pipeline-name">{name}</div>
                {status_badge}
            </div>
            <div class="pipeline-meta">
                Last run: {last_run_str} | Rows: {row_count:,}
            </div>
            {f'<div class="pipeline-meta">{description}</div>' if description else ''}
        </div>
    """, unsafe_allow_html=True)


def render_log_viewer(logs: List[Dict[str, Any]], max_lines: int = 100) -> None:
    """Render a log viewer component."""
    if not logs:
        st.info("No logs available")
        return
    
    log_html = []
    for log in logs[-max_lines:]:
        level = log.get("level", "info").lower()
        timestamp = log.get("timestamp", "")
        message = log.get("message", "")
        
        log_html.append(
            f'<div class="log-line {level}">'
            f'<span style="color: #64748b;">[{timestamp}]</span> '
            f'{message}'
            f'</div>'
        )
    
    st.markdown(f"""
        <div class="log-container">
            {''.join(log_html)}
        </div>
    """, unsafe_allow_html=True)


def render_config_editor(
    config_content: str,
    key: str = "config_editor",
) -> str:
    """Render a configuration editor and return updated content."""
    return st.text_area(
        "Configuration (YAML)",
        value=config_content,
        height=400,
        key=key,
        help="Edit pipeline configuration in YAML format",
    )


def render_progress_bar(
    current: int,
    total: int,
    label: str = "Progress",
) -> None:
    """Render a progress bar with label."""
    if total == 0:
        progress = 0
    else:
        progress = min(current / total, 1.0)
    
    percentage = int(progress * 100)
    
    st.markdown(f"""
        <div style="margin-bottom: 0.5rem;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 0.25rem;">
                <span style="font-size: 0.875rem; color: #64748b;">{label}</span>
                <span style="font-size: 0.875rem; font-weight: 600;">{percentage}%</span>
            </div>
            <div style="background: #e2e8f0; border-radius: 999px; height: 8px; overflow: hidden;">
                <div style="background: linear-gradient(90deg, #3b82f6, #8b5cf6); height: 100%; width: {percentage}%; transition: width 0.3s;"></div>
            </div>
            <div style="font-size: 0.75rem; color: #94a3b8; margin-top: 0.25rem;">
                {current:,} / {total:,} rows
            </div>
        </div>
    """, unsafe_allow_html=True)


def render_alert(
    message: str,
    alert_type: str = "info",
) -> None:
    """Render an alert box."""
    icons = {
        "info": "i",
        "success": "check",
        "warning": "!",
        "error": "x",
    }
    
    st.markdown(f"""
        <div class="alert alert-{alert_type}">
            {message}
        </div>
    """, unsafe_allow_html=True)


def render_stats_row(stats: Dict[str, Any]) -> None:
    """Render a row of statistics."""
    cols = st.columns(len(stats))
    
    for col, (label, value) in zip(cols, stats.items()):
        with col:
            render_metric_card(label, value)


def render_run_history_table(history: List[Dict[str, Any]]) -> None:
    """Render a table of pipeline run history."""
    if not history:
        st.info("No run history available")
        return
    
    import pandas as pd
    
    df = pd.DataFrame(history)
    
    # Format columns
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    
    if "duration" in df.columns:
        df["duration"] = df["duration"].apply(lambda x: f"{x:.2f}s")
    
    if "rows" in df.columns:
        df["rows"] = df["rows"].apply(lambda x: f"{x:,}")
    
    st.dataframe(df, use_container_width=True, hide_index=True)


def render_source_sink_diagram(
    source_type: str,
    transformers: List[str],
    sink_type: str,
) -> None:
    """Render a simple pipeline flow diagram."""
    flow_items = [f"[{source_type.upper()}]"]
    
    for t in transformers:
        flow_items.append(f"[{t}]")
    
    flow_items.append(f"[{sink_type.upper()}]")
    
    flow_html = ' &rarr; '.join(flow_items)
    
    st.markdown(f"""
        <div style="
            background: #f8fafc;
            border-radius: 8px;
            padding: 1rem;
            font-family: monospace;
            font-size: 0.875rem;
            overflow-x: auto;
            white-space: nowrap;
        ">
            {flow_html}
        </div>
    """, unsafe_allow_html=True)
