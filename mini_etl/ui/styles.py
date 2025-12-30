"""
Custom CSS styles for the mini-etl UI.

Professional, clean design without emojis.
"""


def get_custom_css() -> str:
    """Return custom CSS for the Streamlit app."""
    return """
    <style>
    /* Import professional font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Global styles */
    .stApp {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Header styling */
    .main-header {
        font-size: 2rem;
        font-weight: 700;
        color: #1a1a2e;
        margin-bottom: 0.5rem;
        letter-spacing: -0.02em;
    }
    
    .sub-header {
        font-size: 1rem;
        color: #64748b;
        margin-bottom: 2rem;
    }
    
    /* Dark mode support */
    @media (prefers-color-scheme: dark) {
        .main-header {
            color: #f1f5f9;
        }
        .sub-header {
            color: #94a3b8;
        }
        .metric-card {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border-color: #334155;
        }
        .metric-value {
            color: #f1f5f9;
        }
        .metric-label {
            color: #94a3b8;
        }
    }
    
    /* Metric cards */
    .metric-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        transition: transform 0.2s, box-shadow 0.2s;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1a1a2e;
        line-height: 1.2;
    }
    
    .metric-label {
        font-size: 0.875rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-top: 0.25rem;
    }
    
    /* Status indicators */
    .status-running {
        color: #3b82f6;
        background: rgba(59, 130, 246, 0.1);
        padding: 0.25rem 0.75rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .status-success {
        color: #10b981;
        background: rgba(16, 185, 129, 0.1);
        padding: 0.25rem 0.75rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .status-error {
        color: #ef4444;
        background: rgba(239, 68, 68, 0.1);
        padding: 0.25rem 0.75rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .status-idle {
        color: #6b7280;
        background: rgba(107, 114, 128, 0.1);
        padding: 0.25rem 0.75rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    /* Log viewer */
    .log-container {
        background: #0f172a;
        border-radius: 8px;
        padding: 1rem;
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 0.8rem;
        line-height: 1.6;
        max-height: 400px;
        overflow-y: auto;
    }
    
    .log-line {
        color: #e2e8f0;
    }
    
    .log-line.info {
        color: #60a5fa;
    }
    
    .log-line.warning {
        color: #fbbf24;
    }
    
    .log-line.error {
        color: #f87171;
    }
    
    /* Pipeline card */
    .pipeline-card {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 0.75rem;
        cursor: pointer;
        transition: all 0.2s;
    }
    
    .pipeline-card:hover {
        border-color: #3b82f6;
        box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
    }
    
    .pipeline-name {
        font-weight: 600;
        color: #1e293b;
        font-size: 1rem;
    }
    
    .pipeline-meta {
        color: #64748b;
        font-size: 0.8rem;
        margin-top: 0.25rem;
    }
    
    /* Config editor */
    .config-editor {
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        overflow: hidden;
    }
    
    .config-editor textarea {
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 0.875rem;
    }
    
    /* Buttons */
    .stButton > button {
        font-weight: 500;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        transition: all 0.2s;
    }
    
    .stButton > button:hover {
        transform: translateY(-1px);
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Sidebar styling */
    .css-1d391kg {
        padding-top: 2rem;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 0.75rem 1.5rem;
        font-weight: 500;
    }
    
    /* Data table styling */
    .dataframe {
        font-size: 0.875rem;
    }
    
    .dataframe th {
        background: #f8fafc;
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.05em;
    }
    
    /* Alert boxes */
    .alert {
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    
    .alert-info {
        background: rgba(59, 130, 246, 0.1);
        border-left: 4px solid #3b82f6;
    }
    
    .alert-success {
        background: rgba(16, 185, 129, 0.1);
        border-left: 4px solid #10b981;
    }
    
    .alert-warning {
        background: rgba(245, 158, 11, 0.1);
        border-left: 4px solid #f59e0b;
    }
    
    .alert-error {
        background: rgba(239, 68, 68, 0.1);
        border-left: 4px solid #ef4444;
    }
    </style>
    """


def get_dark_theme_css() -> str:
    """Return additional CSS for dark theme."""
    return """
    <style>
    .stApp {
        background-color: #0f172a;
    }
    
    .main-header {
        color: #f1f5f9;
    }
    
    .sub-header {
        color: #94a3b8;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border-color: #334155;
    }
    
    .metric-value {
        color: #f1f5f9;
    }
    
    .metric-label {
        color: #94a3b8;
    }
    
    .pipeline-card {
        background: #1e293b;
        border-color: #334155;
    }
    
    .pipeline-name {
        color: #f1f5f9;
    }
    
    .pipeline-meta {
        color: #94a3b8;
    }
    </style>
    """
