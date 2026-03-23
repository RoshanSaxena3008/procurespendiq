
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))

import html
import re
import json
import math
import os
import logging
import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Optional

# Initialize logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

from config import Config

try:
    from ui_styling import apply_professional_styling, render_header, render_metric_card
    from genie_contextual_memory import ContextualMemoryManager
    from llm_service_enhanced import generate_sql, get_memory_stats
    ENHANCED_UI_AVAILABLE = True
except ImportError:
    logger.warning("v2.1 enhancement modules not found. Using standard UI.")
    ENHANCED_UI_AVAILABLE = False

from db_service import (
    get_active_session, 
    run_df, 
    execute_query, 
    execute_non_query, 
    normalize_upper, 
    run_warehouse_df, 
    run_warehouse_non_query, 
    get_warehouse_connection
)

try:
    from llm_service_full import (
        generate_sql as generate_sql_original,
        cortex_complete, 
        generate_prescriptive_insights, 
        generate_ai_invoice_suggestion
    )
except ImportError:
    # Fall back to enhanced version if original not available
    from llm_service_enhanced import (
        generate_sql as generate_sql_original,
        cortex_complete,
        generate_prescriptive_insights,
        generate_ai_invoice_suggestion
    )

import altair as alt
import urllib.parse
from db_service import cache_get, cache_set
from warehouse_setup import ensure_warehouse_tables

session = get_active_session()

# ---------- Dependencies Check ----------
try:
    import streamlit as st
    import pandas as pd
    import altair as alt
    import numpy as np
except ImportError as e:
    st.error(f"Missing dependency: {e}. Please install required packages.")
    st.stop()

# ---------- Page Config ----------
PAGE_ICON_URL = Config.PAGE_ICON_URL

# Set page config with enhanced defaults
st.set_page_config(
    page_title=Config.APP_TITLE,
    layout="wide",
    page_icon=PAGE_ICON_URL,
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://docs.procureiq.local',
        'Report a bug': 'https://github.com/procureiq/issues',
        'About': f'{Config.APP_TITLE} v2.1 - Intelligent Procurement & Spend Analytics'
    }
)

# Apply professional styling if available
if ENHANCED_UI_AVAILABLE:
    apply_professional_styling()

# ---------- Fabric Session ----------
session = get_active_session()

if "startup_db_check_done" not in st.session_state:
    st.session_state["startup_db_check_done"] = True
    try:
        session.sql("SELECT 1 AS probe").collect()

        if "warehouse_setup_done" not in st.session_state:
            try:
                _setup_results = ensure_warehouse_tables()
                st.session_state["warehouse_setup_done"] = True
                _setup_errors = {k: v for k, v in _setup_results.items() if "error" in str(v)}
                if _setup_errors:
                    st.warning(f"Some warehouse tables could not be created: {_setup_errors}")
            except Exception as _setup_err:
                st.warning(f"Warehouse setup warning (non-blocking): {_setup_err}")
                st.session_state["warehouse_setup_done"] = True

        if "validation_run" not in st.session_state:
            try:
                from data_validation import run_all_validations
                logger.info("Running startup data validation...")
                validation_results = run_all_validations(persist=True)
                st.session_state["validation_run"] = True
                passed = sum(1 for r in validation_results if r.status == "PASS")
                failed = sum(1 for r in validation_results if r.status == "FAIL")
                errors  = sum(1 for r in validation_results if r.status == "ERROR")
                logger.info(f"Validation complete: {passed} passed, {failed} failed, {errors} errors")
            except Exception as validation_error:
                logger.warning(f"Data validation failed (non-blocking): {validation_error}")

    except Exception as e:
        err_str = str(e)
        st.error("Database connection failed. Check the diagnostics below.")
        if "Server is not found" in err_str or "connection to ." in err_str or "08001" in err_str:
            st.markdown(
                """
**Likely cause: FABRIC_SQL_SERVER is blank or unreachable.**

Run this in your VS Code terminal to diagnose:
```
python -c "from config import Config; Config.print_diagnostics()"
```

Common fixes:
1. Make sure `.env` is in the **same folder** as `app.py`
2. Values in `.env` must have **no surrounding quotes**:
   - Correct: `FABRIC_SQL_SERVER=server.fabric.microsoft.com`
   - Wrong:   `FABRIC_SQL_SERVER="server.fabric.microsoft.com"`
3. Re-run `streamlit run app.py` after editing `.env`
                """,
                unsafe_allow_html=False,
            )
        elif "Login failed" in err_str or "18456" in err_str:
            st.markdown(
                """
**Likely cause: Service Principal credentials are wrong.**

Check `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET` in your `.env` file.
                """
            )
        elif "Login timeout" in err_str:
            st.markdown(
                """
**Likely cause: Network / firewall is blocking the connection.**

- Confirm you are on the corporate VPN or the Fabric workspace allows your IP.
- Verify the server address ends with `.datawarehouse.fabric.microsoft.com`.
                """
            )
        with st.expander("Full error details"):
            st.code(err_str)

# ---------- Fabric Configuration ----------
DB = Config.FABRIC_DATABASE
SCHEMA = Config.SCHEMA
SEMANTIC_MODEL_FILE = Config.SCHEMA_METADATA_FILE
GENIE_HISTORY_TABLE = f"{DB}.{SCHEMA}.genie_question_history"

# ---------- Initialize v2.1 Enhancements ----------
if ENHANCED_UI_AVAILABLE:
    # Initialize memory manager
    if "memory_manager" not in st.session_state:
        st.session_state.memory_manager = ContextualMemoryManager(st.session_state)
        st.session_state.memory_manager.initialize_session_memory()

# ---------- Render Enhanced Header ----------
if ENHANCED_UI_AVAILABLE:
    render_header(
        title="ProcureIQ Analytics",
        subtitle="Intelligent Procurement & Spend Management Dashboard v2.1"
    )
else:
    st.title("ProcureIQ Analytics")
    st.markdown("Intelligent Procurement & Spend Management Dashboard")

# Display system status
col1, col2, col3 = st.columns(3)
with col1:
    st.write(f"**Last Updated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    st.write("Status: All Systems Operational")
with col3:
    if ENHANCED_UI_AVAILABLE:
        memory_stats = get_memory_stats()
        st.write(f"**Memory:** {memory_stats['short_term_messages']} messages | {memory_stats['long_term_contexts']} contexts")

st.markdown("---")

# ---------- Auto-suspend idle session ----------
try:
    from auto_suspend import inject_idle_timer
    inject_idle_timer()
except ImportError:
    logger.warning("Auto-suspend module not available")

def _sql_escape(s: str) -> str:
    """Escape single quotes for SQL string literal."""
    return (s or "").replace("'", "''")

saved_insights_TABLE = f"{Config.WAREHOUSE_SCHEMA}.{Config.SAVED_INSIGHTS_TABLE}"

def _resolve_user_identity() -> str:
    """
    Resolve the current user identity through a prioritised chain.
    """
    cached = st.session_state.get("_resolved_user")
    if cached:
        return cached

    def _cache(val: str) -> str:
        st.session_state["_resolved_user"] = val
        return val

    def _clean(val) -> str:
        if val is None:
            return ""
        s = str(val).strip()
        return "" if s in ("None", "nan", "null", "<NA>", "") else s

    # Try Fabric SQL
    try:
        df = session.sql("""
            SELECT COALESCE(
                TRIM(CURRENT_USER()),
                TRIM(SYS_CONTEXT('Fabric$SESSION', 'PRINCIPAL_NAME')),
                ''
            ) AS SF_USER
        """).to_pandas()
        if not df.empty and "SF_USER" in df.columns:
            val = _clean(df.at[0, "SF_USER"])
            if val:
                return _cache(val)
    except Exception:
        pass

    # Try Azure App Service header
    try:
        import os
        val = _clean(os.getenv("HTTP_X_MS_CLIENT_PRINCIPAL_NAME"))
        if val:
            return _cache(val)
    except Exception:
        pass

    # Try environment variable
    try:
        import os
        val = _clean(os.getenv("APP_USER"))
        if val:
            return _cache(val)
    except Exception:
        pass

    # Fall back to OS login
    try:
        import getpass
        return _cache(getpass.getuser())
    except Exception:
        pass

    # Last resort: hostname
    try:
        import socket
        return _cache(socket.gethostname())
    except Exception:
        return _cache("UNKNOWN")

# Get current user
CURRENT_USER = _resolve_user_identity()

# ---------- Sidebar Navigation ----------
st.sidebar.markdown("### 🎯 Dashboard Navigation")
view = st.sidebar.radio(
    "Select View",
    ["Overview", "Genie", "History", "Saved Insights", "Settings"],
    label_visibility="collapsed"
)

st.sidebar.markdown("---")

# Memory stats in sidebar
if ENHANCED_UI_AVAILABLE:
    st.sidebar.markdown("### 📊 Memory Stats")
    memory = st.session_state.memory_manager.get_memory_stats()
    st.sidebar.info(f"""
    **Short-term:** {memory['short_term_messages']} messages
    
    **Long-term:** {memory['long_term_contexts']} contexts
    
    **Memory Enabled:** {memory['memory_enabled']}
    """)

st.sidebar.markdown("---")

# ---------- Main Views ----------

if view == "Overview":
    st.markdown("## Overview Dashboard")
    st.write("Welcome to ProcureIQ Analytics")
    
    if ENHANCED_UI_AVAILABLE:
        # Display sample metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            render_metric_card("Total Spend", "$2.5M", "+12.5% YoY", "success")
        with col2:
            render_metric_card("Vendors", "287", "-5 vendors", "info")
        with col3:
            render_metric_card("Transactions", "12,450", "+2.3%", "warning")
        with col4:
            render_metric_card("On-Time Rate", "92.5%", "+1.2%", "success")
    else:
        st.write("Professional metrics would display here with enhanced UI.")

elif view == "Genie":
    st.markdown("## Genie - AI-Powered Query Assistant")
    
    st.write("Ask a natural language question about your procurement data:")
    question = st.text_area(
        "Your Question",
        placeholder="e.g., 'Show me the top 5 vendors by spend in the last 6 months'",
        height=100,
        label_visibility="collapsed"
    )
    
    col1, col2, col3 = st.columns(3)
    with col1:
        generate_btn = st.button("Generate SQL", use_container_width=True)
    with col2:
        clear_btn = st.button("Clear Memory", use_container_width=True)
    with col3:
        show_memory = st.button("Show Memory", use_container_width=True)
    
    if clear_btn and ENHANCED_UI_AVAILABLE:
        st.session_state.memory_manager.clear_session_memory()
        st.success("Session memory cleared!")
    
    if show_memory and ENHANCED_UI_AVAILABLE:
        memory = st.session_state.memory_manager
        short_context = memory.get_short_term_context()
        if short_context:
            st.text_area("Short-term Context", short_context, height=200)
    
    if generate_btn and question:
        with st.spinner("Generating SQL..."):
            try:
                # Use enhanced version if available
                if ENHANCED_UI_AVAILABLE:
                    sql = generate_sql(
                        question=question,
                        user_id=CURRENT_USER,
                        session_id=st.session_id,
                        include_context=True
                    )
                else:
                    sql = generate_sql_original(question)
                
                st.success("SQL Generated!")
                st.code(sql, language="sql")
                
                # Execute button
                if st.button("Execute Query"):
                    try:
                        result = run_df(sql)
                        st.dataframe(result, use_container_width=True)
                        st.success(f"Query returned {len(result)} rows")
                    except Exception as e:
                        st.error(f"Error executing query: {e}")
            except Exception as e:
                st.error(f"Error: {e}")

elif view == "History":
    st.markdown("## Genie History")
    
    try:
        history_query = f"""
        SELECT TOP 20 
            question,
            frequency,
            last_used,
            is_verified
        FROM {GENIE_HISTORY_TABLE}
        ORDER BY last_used DESC
        """
        history_df = run_warehouse_df(history_query)
        
        if not history_df.empty:
            st.dataframe(history_df, use_container_width=True)
        else:
            st.info("No query history found")
    except Exception as e:
        st.warning(f"Could not load history: {e}")

elif view == "Saved Insights":
    st.markdown("## Saved Insights")
    
    try:
        insights_query = f"""
        SELECT TOP 20
            insight_id,
            title,
            insight_text,
            created_at,
            created_by
        FROM {saved_insights_TABLE}
        ORDER BY created_at DESC
        """
        insights_df = run_warehouse_df(insights_query)
        
        if not insights_df.empty:
            st.dataframe(insights_df, use_container_width=True)
        else:
            st.info("No saved insights found")
    except Exception as e:
        st.warning(f"Could not load insights: {e}")

elif view == "Settings":
    st.markdown("## Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Configuration")
        st.write(f"**Brand Name:** {Config.BRAND_NAME}")
        st.write(f"**Database:** {Config.FABRIC_DATABASE}")
        st.write(f"**Schema:** {Config.SCHEMA}")
        st.write(f"**Current User:** {CURRENT_USER}")
    
    with col2:
        st.subheader("v2.1 Features")
        if ENHANCED_UI_AVAILABLE:
            st.write(" Professional UI/UX")
            st.write(f" Short-term Memory: {'Enabled' if Config.SHORT_TERM_MEMORY_ENABLED else 'Disabled'}")
            st.write(f" Long-term Memory: {'Enabled' if Config.LONG_TERM_MEMORY_ENABLED else 'Disabled'}")
            st.write(f" Context Window: {Config.GENIE_CONTEXT_WINDOW_SIZE} tokens")
        else:
            st.write(" Enhancement modules not loaded")
    
    if st.button("Run Diagnostics"):
        st.write("Configuration Diagnostics:")
        for line in Config.validate_connection_values():
            st.write(line)

# ---------- Footer ----------
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #718096; font-size: 0.85rem; margin-top: 2rem;">
    <p>ProcureIQ Analytics v2.1 | Professional UI + Contextual Memory + Enhanced Genie</p>
    <p>© 2024 - All Rights Reserved</p>
</div>
""", unsafe_allow_html=True)
