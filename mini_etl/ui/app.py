"""
Mini-ETL Monitoring Dashboard

A professional, clean Streamlit interface for managing ETL pipelines.
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import time
import threading

import streamlit as st

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from mini_etl.ui.styles import get_custom_css, get_dark_theme_css
from mini_etl.ui.components import (
    render_metric_card,
    render_status_badge,
    render_pipeline_card,
    render_log_viewer,
    render_config_editor,
    render_progress_bar,
    render_alert,
    render_stats_row,
    render_run_history_table,
    render_source_sink_diagram,
)


# Page configuration
st.set_page_config(
    page_title="Mini-ETL Dashboard",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# Apply custom styles
st.markdown(get_custom_css(), unsafe_allow_html=True)


def init_session_state():
    """Initialize session state variables."""
    if "pipelines" not in st.session_state:
        st.session_state.pipelines = []
    
    if "run_history" not in st.session_state:
        st.session_state.run_history = []
    
    if "logs" not in st.session_state:
        st.session_state.logs = []
    
    if "current_pipeline" not in st.session_state:
        st.session_state.current_pipeline = None
    
    if "is_running" not in st.session_state:
        st.session_state.is_running = False
    
    if "dark_mode" not in st.session_state:
        st.session_state.dark_mode = False


def add_log(message: str, level: str = "info"):
    """Add a log entry."""
    st.session_state.logs.append({
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "level": level,
        "message": message,
    })


def render_sidebar():
    """Render the sidebar navigation."""
    with st.sidebar:
        st.markdown('<h2 style="margin-bottom: 1rem;">Mini-ETL</h2>', unsafe_allow_html=True)
        st.markdown('<p style="color: #64748b; font-size: 0.875rem; margin-bottom: 2rem;">Pipeline Management</p>', unsafe_allow_html=True)
        
        # Theme toggle
        dark_mode = st.toggle("Dark Mode", value=st.session_state.dark_mode, key="theme_toggle")
        if dark_mode != st.session_state.dark_mode:
            st.session_state.dark_mode = dark_mode
            st.rerun()
        
        st.markdown("---")
        
        # Navigation
        page = st.radio(
            "Navigation",
            ["Dashboard", "Run Pipeline", "Configuration", "History", "Settings"],
            label_visibility="collapsed",
        )
        
        st.markdown("---")
        
        # Quick stats
        st.markdown("**Quick Stats**")
        total_runs = len(st.session_state.run_history)
        success_runs = sum(1 for r in st.session_state.run_history if r.get("status") == "success")
        
        st.caption(f"Total Runs: {total_runs}")
        st.caption(f"Successful: {success_runs}")
        
        return page


def render_dashboard():
    """Render the main dashboard page."""
    st.markdown('<h1 class="main-header">Dashboard</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Overview of your ETL pipelines</p>', unsafe_allow_html=True)
    
    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        render_metric_card("Total Runs", len(st.session_state.run_history))
    
    with col2:
        success_count = sum(1 for r in st.session_state.run_history if r.get("status") == "success")
        render_metric_card("Successful", success_count)
    
    with col3:
        total_rows = sum(r.get("rows", 0) for r in st.session_state.run_history)
        render_metric_card("Rows Processed", f"{total_rows:,}")
    
    with col4:
        if st.session_state.run_history:
            avg_duration = sum(r.get("duration", 0) for r in st.session_state.run_history) / len(st.session_state.run_history)
            render_metric_card("Avg Duration", f"{avg_duration:.1f}s")
        else:
            render_metric_card("Avg Duration", "N/A")
    
    st.markdown("---")
    
    # Recent activity
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### Recent Runs")
        if st.session_state.run_history:
            render_run_history_table(st.session_state.run_history[-5:][::-1])
        else:
            st.info("No pipeline runs yet. Go to 'Run Pipeline' to execute a pipeline.")
    
    with col2:
        st.markdown("### Logs")
        render_log_viewer(st.session_state.logs)


def render_run_pipeline():
    """Render the run pipeline page."""
    st.markdown('<h1 class="main-header">Run Pipeline</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Execute ETL pipelines from configuration</p>', unsafe_allow_html=True)
    
    # File upload or paste config
    tab1, tab2 = st.tabs(["Upload Config", "Paste Config"])
    
    with tab1:
        uploaded_file = st.file_uploader(
            "Upload configuration file",
            type=["yaml", "yml", "json"],
            help="Upload a YAML or JSON pipeline configuration file",
        )
        
        if uploaded_file:
            config_content = uploaded_file.read().decode("utf-8")
            st.session_state.current_config = config_content
            st.success(f"Loaded: {uploaded_file.name}")
    
    with tab2:
        from mini_etl.core.config import generate_sample_config
        
        default_config = st.session_state.get("current_config", generate_sample_config())
        
        config_content = st.text_area(
            "Pipeline Configuration (YAML)",
            value=default_config,
            height=350,
            help="Enter your pipeline configuration in YAML format",
        )
        st.session_state.current_config = config_content
    
    st.markdown("---")
    
    # Run controls
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        validate_btn = st.button("Validate", use_container_width=True)
    
    with col2:
        run_btn = st.button(
            "Run Pipeline",
            type="primary",
            use_container_width=True,
            disabled=st.session_state.is_running,
        )
    
    # Validation
    if validate_btn:
        try:
            from mini_etl.core.config import ConfigLoader
            
            loader = ConfigLoader()
            config = loader.load_string(st.session_state.get("current_config", ""))
            errors = config.validate()
            
            if errors:
                for error in errors:
                    st.error(error)
            else:
                st.success("Configuration is valid")
                
                # Show pipeline diagram
                st.markdown("#### Pipeline Flow")
                transformers = [t.type for t in config.transformers]
                render_source_sink_diagram(
                    config.source.type if config.source else "unknown",
                    transformers,
                    config.sink.type if config.sink else "unknown",
                )
        except Exception as e:
            st.error(f"Validation error: {e}")
    
    # Run pipeline
    if run_btn:
        run_pipeline_with_progress()


def run_pipeline_with_progress():
    """Run the pipeline with progress updates."""
    try:
        from mini_etl.core.config import ConfigLoader
        
        st.session_state.is_running = True
        add_log("Starting pipeline execution...", "info")
        
        loader = ConfigLoader()
        config = loader.load_string(st.session_state.get("current_config", ""))
        
        errors = config.validate()
        if errors:
            for error in errors:
                st.error(error)
                add_log(f"Validation error: {error}", "error")
            st.session_state.is_running = False
            return
        
        pipeline = loader.build_pipeline(config)
        pipeline.show_progress = False  # We handle progress in UI
        
        add_log(f"Running pipeline: {config.name}", "info")
        
        progress_placeholder = st.empty()
        status_placeholder = st.empty()
        
        start_time = time.time()
        
        with progress_placeholder:
            st.info("Executing pipeline...")
        
        # Run pipeline
        stats = pipeline.run()
        
        duration = time.time() - start_time
        
        with progress_placeholder:
            st.success(f"Pipeline completed in {duration:.2f}s")
        
        with status_placeholder:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Rows Processed", f"{stats['rows']:,}")
            with col2:
                st.metric("Chunks", stats['chunks'])
            with col3:
                st.metric("Throughput", f"{stats['rows_per_second']:,.0f} rows/sec")
        
        # Add to history
        st.session_state.run_history.append({
            "timestamp": datetime.now().isoformat(),
            "name": config.name,
            "status": "success",
            "rows": stats["rows"],
            "duration": duration,
        })
        
        add_log(f"Pipeline completed: {stats['rows']} rows processed", "info")
        
    except Exception as e:
        st.error(f"Pipeline failed: {e}")
        add_log(f"Pipeline failed: {e}", "error")
        
        st.session_state.run_history.append({
            "timestamp": datetime.now().isoformat(),
            "name": "unknown",
            "status": "error",
            "rows": 0,
            "duration": 0,
            "error": str(e),
        })
    finally:
        st.session_state.is_running = False


def render_configuration():
    """Render the configuration management page."""
    st.markdown('<h1 class="main-header">Configuration</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Manage pipeline configurations</p>', unsafe_allow_html=True)
    
    # Sample configs
    st.markdown("### Sample Configurations")
    
    samples = {
        "CSV to Parquet": """pipeline:
  name: csv_to_parquet
  description: Convert CSV to Parquet format
  
  source:
    type: csv
    path: input.csv
    chunksize: 10000
  
  transformers:
    - type: filter
      condition: "value > 0"
  
  sink:
    type: parquet
    path: output.parquet
""",
        "JSON to SQL": """pipeline:
  name: json_to_sql
  description: Load JSON data into SQL database
  
  source:
    type: json
    path: data.jsonl
    lines: true
  
  transformers:
    - type: rename
      columns:
        old_col: new_col
    - type: cast
      columns:
        id: int
        amount: float
  
  sink:
    type: sql
    connection_string: sqlite:///output.db
    table: data
    mode: append
""",
        "API to CSV": """pipeline:
  name: api_to_csv
  description: Fetch data from API and save to CSV
  
  source:
    type: api
    url: https://api.example.com/data
    data_path: results
    pagination:
      type: page
      page_param: page
      limit: 100
  
  transformers:
    - type: select
      columns:
        - id
        - name
        - value
  
  sink:
    type: csv
    path: api_data.csv
""",
    }
    
    selected_sample = st.selectbox("Choose a sample", list(samples.keys()))
    
    if st.button("Load Sample"):
        st.session_state.current_config = samples[selected_sample]
        st.success(f"Loaded '{selected_sample}' configuration")
        st.code(samples[selected_sample], language="yaml")


def render_history():
    """Render the run history page."""
    st.markdown('<h1 class="main-header">Run History</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">View past pipeline executions</p>', unsafe_allow_html=True)
    
    if not st.session_state.run_history:
        st.info("No pipeline runs recorded yet.")
        return
    
    # Filters
    col1, col2 = st.columns([1, 3])
    
    with col1:
        status_filter = st.selectbox(
            "Filter by Status",
            ["All", "Success", "Error"],
        )
    
    # Filter history
    filtered_history = st.session_state.run_history
    if status_filter != "All":
        filtered_history = [
            r for r in filtered_history 
            if r.get("status", "").lower() == status_filter.lower()
        ]
    
    # Stats
    st.markdown("### Summary")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Runs", len(filtered_history))
    
    with col2:
        success_count = sum(1 for r in filtered_history if r.get("status") == "success")
        st.metric("Successful", success_count)
    
    with col3:
        total_rows = sum(r.get("rows", 0) for r in filtered_history)
        st.metric("Total Rows", f"{total_rows:,}")
    
    with col4:
        if filtered_history:
            total_duration = sum(r.get("duration", 0) for r in filtered_history)
            st.metric("Total Time", f"{total_duration:.1f}s")
    
    st.markdown("---")
    
    # History table
    st.markdown("### Run Details")
    render_run_history_table(filtered_history[::-1])
    
    # Clear history button
    if st.button("Clear History", type="secondary"):
        st.session_state.run_history = []
        st.rerun()


def render_settings():
    """Render the settings page."""
    st.markdown('<h1 class="main-header">Settings</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Configure application preferences</p>', unsafe_allow_html=True)
    
    st.markdown("### Display")
    
    dark_mode = st.checkbox(
        "Dark Mode",
        value=st.session_state.dark_mode,
        help="Enable dark color scheme",
    )
    
    if dark_mode != st.session_state.dark_mode:
        st.session_state.dark_mode = dark_mode
        st.rerun()
    
    st.markdown("---")
    
    st.markdown("### Logging")
    
    max_logs = st.slider(
        "Maximum log entries",
        min_value=50,
        max_value=500,
        value=100,
        step=50,
    )
    
    if st.button("Clear Logs"):
        st.session_state.logs = []
        st.success("Logs cleared")
    
    st.markdown("---")
    
    st.markdown("### About")
    
    from mini_etl import __version__
    
    st.markdown(f"""
    **Mini-ETL Dashboard**
    
    Version: {__version__}
    
    A lightweight, streaming ETL system in Python.
    
    [Documentation](https://github.com/adjei/mini-etl) | 
    [Report Issue](https://github.com/adjei/mini-etl/issues)
    """)


def run_app():
    """Main application entry point."""
    init_session_state()
    
    # Apply dark theme if enabled
    if st.session_state.dark_mode:
        st.markdown(get_dark_theme_css(), unsafe_allow_html=True)
    
    # Render sidebar and get current page
    page = render_sidebar()
    
    # Render selected page
    if page == "Dashboard":
        render_dashboard()
    elif page == "Run Pipeline":
        render_run_pipeline()
    elif page == "Configuration":
        render_configuration()
    elif page == "History":
        render_history()
    elif page == "Settings":
        render_settings()


# Run the app
if __name__ == "__main__":
    run_app()
