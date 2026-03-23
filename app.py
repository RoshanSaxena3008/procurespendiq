# ================================
# ProcureIQ - Intelligent Procurement Analytics Dashboard
# Enhanced Professional UI/UX Version
# ================================

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "procurespendiq-main"))

import html
import re
import json
import math
import logging
import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Optional
import altair as alt
import urllib.parse

# Initialize logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# ============================================================================
# PAGE CONFIGURATION & THEME
# ============================================================================
st.set_page_config(
    page_title="ProcureIQ - Procurement Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://docs.procureiq.local',
        'Report a bug': 'https://support.procureiq.local',
        'About': 'ProcureIQ v2.0 - Intelligent Procurement & Spend Analytics'
    }
)

# Professional CSS styling
st.markdown("""
    <style>
    /* Root variables */
    :root {
        --primary-color: #667eea;
        --primary-dark: #5568d3;
        --secondary-color: #764ba2;
        --success-color: #28a745;
        --warning-color: #ffc107;
        --danger-color: #dc3545;
        --info-color: #17a2b8;
        --light-bg: #f8f9fa;
        --white: #ffffff;
        --text-primary: #2d3748;
        --text-secondary: #718096;
        --border-color: #e2e8f0;
    }
    
    /* Main container */
    .main {
        background-color: var(--light-bg);
    }
    
    /* Header container */
    .header-container {
        background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%);
        padding: 2.5rem;
        border-radius: 12px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.2);
    }
    
    .header-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
        color: white;
        display: flex;
        align-items: center;
        gap: 1rem;
    }
    
    .header-subtitle {
        font-size: 1.1rem;
        opacity: 0.95;
        margin-top: 0.75rem;
        color: rgba(255, 255, 255, 0.9);
    }
    
    /* Metric cards */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border-left: 4px solid var(--primary-color);
        transition: transform 0.2s, box-shadow 0.2s;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.12);
    }
    
    .metric-label {
        color: var(--text-secondary);
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: var(--text-primary);
        margin-top: 0.5rem;
    }
    
    .metric-change {
        font-size: 0.85rem;
        color: var(--success-color);
        margin-top: 0.5rem;
    }
    
    /* Section headers */
    .section-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid var(--border-color);
        padding-bottom: 0.75rem;
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }
    
    .subsection-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-top: 1.5rem;
        margin-bottom: 0.75rem;
    }
    
    /* Cards and containers */
    .card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        margin-bottom: 1rem;
    }
    
    .card-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 1rem;
        border-bottom: 1px solid var(--border-color);
        padding-bottom: 0.75rem;
    }
    
    /* Alerts */
    .alert {
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid;
        margin-bottom: 1rem;
    }
    
    .alert-success {
        background-color: #d4edda;
        border-color: var(--success-color);
        color: #155724;
    }
    
    .alert-warning {
        background-color: #fff3cd;
        border-color: var(--warning-color);
        color: #856404;
    }
    
    .alert-danger {
        background-color: #f8d7da;
        border-color: var(--danger-color);
        color: #721c24;
    }
    
    .alert-info {
        background-color: #d1ecf1;
        border-color: var(--info-color);
        color: #0c5460;
    }
    
    /* Buttons */
    .stButton>button {
        border-radius: 8px;
        font-weight: 600;
        padding: 0.5rem 1.5rem;
        border: none;
        transition: all 0.3s;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
    }
    
    /* Sidebar styling */
    .sidebar .sidebar-content {
        background-color: white;
    }
    
    .sidebar-header {
        font-size: 1.1rem;
        font-weight: 700;
        color: var(--primary-color);
        margin-top: 1.5rem;
        margin-bottom: 1rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Tables */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: var(--light-bg);
        border-radius: 8px;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] button {
        border-radius: 8px 8px 0 0;
        font-weight: 600;
    }
    
    /* Badge styling */
    .badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        margin: 0.25rem;
    }
    
    .badge-success {
        background-color: #d4edda;
        color: #155724;
    }
    
    .badge-warning {
        background-color: #fff3cd;
        color: #856404;
    }
    
    .badge-danger {
        background-color: #f8d7da;
        color: #721c24;
    }
    
    .badge-info {
        background-color: #d1ecf1;
        color: #0c5460;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================
def init_session_state():
    """Initialize session state variables"""
    defaults = {
        'data_loaded': False,
        'selected_vendor': None,
        'view_mode': 'dashboard',
        'filter_applied': False,
        'sort_column': 'Amount',
        'sort_order': 'descending'
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
@st.cache_data
def load_sample_data():
    """Load or generate sample procurement data"""
    np.random.seed(42)
    
    # Generate realistic procurement data
    n_records = 500
    dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='D')
    
    categories = ['IT Equipment', 'Office Supplies', 'Facilities', 'Marketing', 
                  'Consulting', 'Travel', 'Software Licenses', 'Professional Services']
    vendors = ['TechCorp Inc', 'Global Supplies Ltd', 'Facilities Pro', 
               'Marketing Solutions', 'Strategy Consulting', 'Travel Express', 
               'SoftServe', 'Enterprise Dynamics']
    statuses = ['Completed', 'In Progress', 'Pending Approval', 'Approved', 'Rejected']
    payment_terms = ['Net 30', 'Net 60', 'Net 90', 'Immediate', '2/10 Net 30']
    
    data = {
        'PO_Number': [f'PO-2024-{str(i).zfill(5)}' for i in range(n_records)],
        'Date': np.random.choice(dates, n_records),
        'Vendor': np.random.choice(vendors, n_records),
        'Category': np.random.choice(categories, n_records),
        'Amount': np.random.uniform(500, 50000, n_records),
        'Status': np.random.choice(statuses, n_records, p=[0.4, 0.25, 0.15, 0.15, 0.05]),
        'Payment_Terms': np.random.choice(payment_terms, n_records),
        'Days_to_Payment': np.random.randint(0, 90, n_records),
        'Department': np.random.choice(['Engineering', 'Operations', 'Finance', 'Sales', 'HR'], n_records)
    }
    
    return pd.DataFrame(data)

@st.cache_data
def load_supplier_metrics():
    """Load supplier performance metrics"""
    suppliers = ['TechCorp Inc', 'Global Supplies Ltd', 'Facilities Pro', 
                 'Marketing Solutions', 'Strategy Consulting', 'Travel Express', 
                 'SoftServe', 'Enterprise Dynamics']
    
    data = {
        'Supplier': suppliers,
        'Total_Spend': [125000, 95000, 87000, 65000, 120000, 55000, 110000, 92000],
        'On_Time_Delivery_%': [95, 88, 92, 85, 98, 82, 90, 87],
        'Quality_Score': [4.5, 4.0, 4.3, 3.8, 4.8, 3.5, 4.2, 4.1],
        'Risk_Level': ['Low', 'Medium', 'Low', 'High', 'Low', 'High', 'Medium', 'Low'],
        'Active_POs': [12, 8, 15, 6, 18, 4, 10, 9]
    }
    
    return pd.DataFrame(data)

def format_currency(value):
    """Format value as currency"""
    return f"${value:,.0f}"

def format_percentage(value):
    """Format value as percentage"""
    return f"{value:.1f}%"

def get_status_badge(status):
    """Return HTML badge for status"""
    status_colors = {
        'Completed': 'success',
        'In Progress': 'info',
        'Pending Approval': 'warning',
        'Approved': 'success',
        'Rejected': 'danger'
    }
    color = status_colors.get(status, 'info')
    return f'<span class="badge badge-{color}">{status}</span>'

def get_risk_badge(risk):
    """Return HTML badge for risk level"""
    risk_colors = {
        'Low': 'success',
        'Medium': 'warning',
        'High': 'danger'
    }
    color = risk_colors.get(risk, 'info')
    return f'<span class="badge badge-{color}">{risk}</span>'

# ============================================================================
# HEADER & NAVIGATION
# ============================================================================
st.markdown("""
    <div class="header-container">
        <div class="header-title">📊 ProcureIQ</div>
        <div class="header-subtitle">Intelligent Procurement & Spend Analytics</div>
    </div>
""", unsafe_allow_html=True)

# Main navigation tabs
col1, col2, col3 = st.columns(3)
with col1:
    st.write("**Last Updated:** " + pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
with col2:
    status_indicator = "🟢 All Systems Operational"
    st.write(status_indicator)
with col3:
    st.write("**Version:** 2.0.0")

st.markdown("---")

# ============================================================================
# SIDEBAR - FILTERS & CONTROLS
# ============================================================================
with st.sidebar:
    st.markdown("### 🎯 Dashboard Controls")
    
    view_mode = st.radio(
        "Select View:",
        ["📊 Overview", "📦 Purchase Orders", "🤝 Suppliers", "💰 Spend Analysis", "⚠️ Alerts"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    st.markdown("### 🔍 Filters")
    
    # Date range filter
    date_range = st.date_input(
        "Date Range",
        value=(date(2024, 1, 1), date(2024, 12, 31)),
        label_visibility="collapsed"
    )
    
    # Category filter
    selected_categories = st.multiselect(
        "Categories",
        ['IT Equipment', 'Office Supplies', 'Facilities', 'Marketing', 
         'Consulting', 'Travel', 'Software Licenses', 'Professional Services'],
        default=['IT Equipment', 'Office Supplies'],
        label_visibility="collapsed"
    )
    
    # Status filter
    selected_statuses = st.multiselect(
        "Status",
        ['Completed', 'In Progress', 'Pending Approval', 'Approved', 'Rejected'],
        default=['Completed', 'Approved'],
        label_visibility="collapsed"
    )
    
    # Vendor filter
    selected_vendors = st.multiselect(
        "Vendors",
        ['TechCorp Inc', 'Global Supplies Ltd', 'Facilities Pro', 
         'Marketing Solutions', 'Strategy Consulting', 'Travel Express', 
         'SoftServe', 'Enterprise Dynamics'],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    # Apply filters button
    if st.button("🔄 Apply Filters", use_container_width=True):
        st.session_state.filter_applied = True
        st.success("Filters applied!")
    
    st.markdown("---")
    st.markdown("### 📚 Help & Support")
    
    with st.expander("❓ FAQ"):
        st.write("""
        **Q: How do I create a new PO?**
        A: Use the Purchase Orders view and click "Create New PO"
        
        **Q: Where can I see supplier metrics?**
        A: Visit the Suppliers view for detailed performance metrics
        
        **Q: How often is data updated?**
        A: Data is refreshed every 15 minutes
        """)
    
    if st.button("📧 Contact Support", use_container_width=True):
        st.info("Support: support@procureiq.local")

# Load data
df = load_sample_data()
suppliers_df = load_supplier_metrics()

# Apply filters
if selected_vendors:
    df = df[df['Vendor'].isin(selected_vendors)]
if selected_categories:
    df = df[df['Category'].isin(selected_categories)]
if selected_statuses:
    df = df[df['Status'].isin(selected_statuses)]

# ============================================================================
# PAGE: OVERVIEW
# ============================================================================
if "Overview" in view_mode:
    st.markdown('<div class="section-header">📈 Executive Dashboard</div>', unsafe_allow_html=True)
    
    # Key Performance Indicators
    col1, col2, col3, col4 = st.columns(4, gap="medium")
    
    total_spend = df['Amount'].sum()
    avg_po_value = df['Amount'].mean()
    total_pos = len(df)
    on_time_rate = suppliers_df['On_Time_Delivery_%'].mean()
    
    with col1:
        st.metric(
            label="💰 Total Spend",
            value=format_currency(total_spend),
            delta="+12.5% YoY",
            delta_color="off"
        )
    
    with col2:
        st.metric(
            label="📊 Avg PO Value",
            value=format_currency(avg_po_value),
            delta="+$2,500 from last month"
        )
    
    with col3:
        st.metric(
            label="📦 Total POs",
            value=f"{total_pos:,}",
            delta="+45 this month"
        )
    
    with col4:
        st.metric(
            label="⏱️ On-Time Rate",
            value=format_percentage(on_time_rate),
            delta="+2.3% improvement"
        )
    
    st.markdown("---")
    
    # Charts section
    st.markdown('<div class="section-header">📊 Analytics & Trends</div>', unsafe_allow_html=True)
    
    chart_col1, chart_col2 = st.columns(2, gap="large")
    
    with chart_col1:
        st.markdown("### Monthly Spend Trend")
        
        # Generate monthly trend
        monthly_data = df.groupby(df['Date'].dt.to_period('M'))['Amount'].sum()
        monthly_data.index = monthly_data.index.to_timestamp()
        
        chart = alt.Chart(monthly_data.reset_index().rename(
            columns={'Date': 'Month', 'Amount': 'Spend'}
        )).mark_area(
            color='#667eea',
            opacity=0.3,
            interpolate='monotone'
        ).encode(
            x='Month:T',
            y='Spend:Q',
            tooltip=['Month:T', 'Spend:Q']
        ).interactive().properties(height=300)
        
        st.altair_chart(chart, use_container_width=True)
    
    with chart_col2:
        st.markdown("### Spend by Category")
        
        category_spend = df.groupby('Category')['Amount'].sum().sort_values(ascending=True)
        
        chart = alt.Chart(category_spend.reset_index().rename(
            columns={'Category': 'Category', 'Amount': 'Spend'}
        )).mark_barh().encode(
            x='Spend:Q',
            y='Category:N',
            color='Spend:Q',
            tooltip=['Category:N', 'Spend:Q']
        ).properties(height=300)
        
        st.altair_chart(chart, use_container_width=True)
    
    st.markdown("---")
    
    # Status distribution and top vendors
    status_col1, status_col2 = st.columns(2, gap="large")
    
    with status_col1:
        st.markdown("### PO Status Distribution")
        
        status_dist = df['Status'].value_counts()
        
        chart = alt.Chart(status_dist.reset_index().rename(
            columns={'Status': 'Status', 'count': 'Count'}
        )).mark_bar().encode(
            x='Count:Q',
            y='Status:N',
            color=alt.Color('Status:N', scale=alt.Scale(scheme='tableau10')),
            tooltip=['Status:N', 'Count:Q']
        ).interactive().properties(height=300)
        
        st.altair_chart(chart, use_container_width=True)
    
    with status_col2:
        st.markdown("### Top 5 Vendors")
        
        top_vendors = df.groupby('Vendor')['Amount'].sum().nlargest(5)
        
        chart = alt.Chart(top_vendors.reset_index().rename(
            columns={'Vendor': 'Vendor', 'Amount': 'Spend'}
        )).mark_bar().encode(
            x='Spend:Q',
            y=alt.Y('Vendor:N', sort='-x'),
            color=alt.Color('Spend:Q', scale=alt.Scale(scheme='blues')),
            tooltip=['Vendor:N', 'Spend:Q']
        ).properties(height=300)
        
        st.altair_chart(chart, use_container_width=True)

# ============================================================================
# PAGE: PURCHASE ORDERS
# ============================================================================
elif "Purchase Orders" in view_mode:
    st.markdown('<div class="section-header">📦 Purchase Orders Management</div>', unsafe_allow_html=True)
    
    # Action buttons
    action_col1, action_col2, action_col3, action_col4 = st.columns(4, gap="small")
    
    with action_col1:
        if st.button("➕ Create New PO", use_container_width=True):
            st.info("✅ New PO creation form would open here")
    
    with action_col2:
        if st.button("📥 Import POs", use_container_width=True):
            st.info("✅ Import dialog would open here")
    
    with action_col3:
        if st.button("📊 Export Report", use_container_width=True):
            st.info("✅ Export options would appear here")
    
    with action_col4:
        if st.button("🔍 Advanced Search", use_container_width=True):
            st.info("✅ Advanced search panel would open here")
    
    st.markdown("---")
    
    # Display PO table
    st.markdown("### Purchase Orders List")
    
    # Sorting options
    col1, col2 = st.columns(2)
    with col1:
        sort_by = st.selectbox("Sort by", ["Amount", "Date", "Status"], key="sort_by")
    with col2:
        sort_order = st.selectbox("Order", ["Descending", "Ascending"], key="sort_order")
    
    # Sort data
    ascending = sort_order == "Ascending"
    df_sorted = df.sort_values(by=sort_by, ascending=ascending)
    
    # Display table
    display_df = df_sorted[[
        'PO_Number', 'Date', 'Vendor', 'Category', 'Amount', 'Status', 'Department'
    ]].head(20).copy()
    
    display_df['Amount'] = display_df['Amount'].apply(format_currency)
    display_df['Date'] = pd.to_datetime(display_df['Date']).dt.strftime('%Y-%m-%d')
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # PO Statistics
    st.markdown("---")
    st.markdown("### PO Statistics")
    
    stats_col1, stats_col2, stats_col3 = st.columns(3)
    
    with stats_col1:
        completed = len(df[df['Status'] == 'Completed'])
        st.metric("Completed POs", completed)
    
    with stats_col2:
        in_progress = len(df[df['Status'] == 'In Progress'])
        st.metric("In Progress", in_progress)
    
    with stats_col3:
        pending = len(df[df['Status'].isin(['Pending Approval', 'Approved'])])
        st.metric("Pending Approval", pending)

# ============================================================================
# PAGE: SUPPLIERS
# ============================================================================
elif "Suppliers" in view_mode:
    st.markdown('<div class="section-header">🤝 Supplier Performance Management</div>', unsafe_allow_html=True)
    
    # Supplier metrics tabs
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "🏆 Rankings", "📋 Details"])
    
    with tab1:
        st.markdown("### Supplier Performance Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Suppliers", len(suppliers_df))
        with col2:
            avg_on_time = suppliers_df['On_Time_Delivery_%'].mean()
            st.metric("Avg On-Time Rate", format_percentage(avg_on_time))
        with col3:
            avg_quality = suppliers_df['Quality_Score'].mean()
            st.metric("Avg Quality Score", f"{avg_quality:.2f}/5.0")
        with col4:
            low_risk = len(suppliers_df[suppliers_df['Risk_Level'] == 'Low'])
            st.metric("Low-Risk Suppliers", low_risk)
        
        st.markdown("---")
        
        # Supplier performance chart
        st.markdown("### Performance Comparison")
        
        chart = alt.Chart(suppliers_df).mark_bar().encode(
            x='On_Time_Delivery_%:Q',
            y=alt.Y('Supplier:N', sort='-x'),
            color=alt.Color('On_Time_Delivery_%:Q', scale=alt.Scale(scheme='greens')),
            tooltip=['Supplier:N', 'On_Time_Delivery_%:Q', 'Quality_Score:Q']
        ).interactive().properties(height=400)
        
        st.altair_chart(chart, use_container_width=True)
    
    with tab2:
        st.markdown("### Supplier Rankings")
        
        ranking_metric = st.selectbox(
            "Rank by:",
            ["Total Spend", "On-Time Delivery", "Quality Score"],
            label_visibility="collapsed"
        )
        
        if ranking_metric == "Total Spend":
            rank_df = suppliers_df.sort_values('Total_Spend', ascending=False)
            display_cols = ['Supplier', 'Total_Spend', 'Active_POs', 'Risk_Level']
        elif ranking_metric == "On-Time Delivery":
            rank_df = suppliers_df.sort_values('On_Time_Delivery_%', ascending=False)
            display_cols = ['Supplier', 'On_Time_Delivery_%', 'Quality_Score', 'Risk_Level']
        else:
            rank_df = suppliers_df.sort_values('Quality_Score', ascending=False)
            display_cols = ['Supplier', 'Quality_Score', 'On_Time_Delivery_%', 'Risk_Level']
        
        rank_df_display = rank_df[display_cols].reset_index(drop=True)
        rank_df_display.index = rank_df_display.index + 1
        
        st.dataframe(rank_df_display, use_container_width=True)
    
    with tab3:
        st.markdown("### Supplier Details")
        
        selected_supplier = st.selectbox(
            "Select Supplier:",
            suppliers_df['Supplier'].unique(),
            label_visibility="collapsed"
        )
        
        supplier_data = suppliers_df[suppliers_df['Supplier'] == selected_supplier].iloc[0]
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Spend", format_currency(supplier_data['Total_Spend']))
            st.metric("Active POs", supplier_data['Active_POs'])
        
        with col2:
            st.metric("On-Time Rate", format_percentage(supplier_data['On_Time_Delivery_%']))
            st.metric("Quality Score", f"{supplier_data['Quality_Score']:.2f}/5.0")
        
        with col3:
            st.markdown(f"**Risk Level:** {get_risk_badge(supplier_data['Risk_Level'])}", 
                       unsafe_allow_html=True)

# ============================================================================
# PAGE: SPEND ANALYSIS
# ============================================================================
elif "Spend Analysis" in view_mode:
    st.markdown('<div class="section-header">💰 Spend Analysis & Insights</div>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["📊 By Dimension", "🎯 Trends", "💡 Insights"])
    
    with tab1:
        st.markdown("### Spend Breakdown")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**By Category**")
            category_spend = df.groupby('Category')['Amount'].sum().sort_values(ascending=False)
            st.bar_chart(category_spend)
        
        with col2:
            st.markdown("**By Department**")
            dept_spend = df.groupby('Department')['Amount'].sum().sort_values(ascending=False)
            st.bar_chart(dept_spend)
    
    with tab2:
        st.markdown("### Spend Trends")
        
        # Monthly spend trend
        monthly_df = df.copy()
        monthly_df['Month'] = pd.to_datetime(monthly_df['Date']).dt.to_period('M').astype(str)
        monthly_spend = monthly_df.groupby('Month')['Amount'].agg(['sum', 'mean', 'count'])
        
        st.line_chart(monthly_spend['sum'])
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Monthly Average Spend**")
            st.area_chart(monthly_spend['mean'])
        with col2:
            st.markdown("**POs per Month**")
            st.bar_chart(monthly_spend['count'])
    
    with tab3:
        st.markdown("### Key Insights & Recommendations")
        
        st.markdown("""
        #### 🔍 Analysis Summary
        
        1. **Top Spending Areas**
           - IT Equipment accounts for 35% of total spend
           - Consulting services showing 15% increase YoY
        
        2. **Vendor Performance**
           - TechCorp Inc: 8 active suppliers with 95% on-time delivery
           - Strategy Consulting: 98% quality score - highest rated
        
        3. **Cost Optimization Opportunities**
           - Consolidate Office Supplies contracts: Potential savings $12,000
           - Negotiate volume discounts with Travel Express: Est. 8% reduction
        
        4. **Risk Indicators**
           - High-risk vendors: 2 suppliers (Monitor closely)
           - Late deliveries increasing in Q4
        """)

# ============================================================================
# PAGE: ALERTS & INSIGHTS
# ============================================================================
elif "Alerts" in view_mode:
    st.markdown('<div class="section-header">⚠️ Alerts & Notifications</div>', unsafe_allow_html=True)
    
    # Alert categories
    tab1, tab2, tab3, tab4 = st.tabs(["🚨 Critical", "⚠️ Warnings", "ℹ️ Info", "✅ Success"])
    
    with tab1:
        st.markdown("""
        <div class="alert alert-danger">
            <strong>🚨 Critical Alert</strong><br>
            High-risk supplier "Travel Express" has 3 failed deliveries this month. 
            <a href="#" style="color: #721c24;"><strong>Review Action Plan</strong></a>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="alert alert-danger">
            <strong>🚨 Budget Alert</strong><br>
            Marketing category spend has exceeded monthly budget by 12%. 
            Immediate review required.
        </div>
        """, unsafe_allow_html=True)
    
    with tab2:
        st.markdown("""
        <div class="alert alert-warning">
            <strong>⚠️ Warning</strong><br>
            5 Purchase Orders pending approval for more than 5 days. 
            Please review and approve.
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="alert alert-warning">
            <strong>⚠️ Performance Alert</strong><br>
            Facilities Pro's on-time delivery rate dropped to 84% (below target of 90%).
        </div>
        """, unsafe_allow_html=True)
    
    with tab3:
        st.markdown("""
        <div class="alert alert-info">
            <strong>ℹ️ Information</strong><br>
            New supplier "Enterprise Dynamics" added. Profile: Low-risk, 4.1 quality score.
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="alert alert-info">
            <strong>ℹ️ System Update</strong><br>
            Data refresh completed at 10:30 AM. Next refresh: 10:45 AM.
        </div>
        """, unsafe_allow_html=True)
    
    with tab4:
        st.markdown("""
        <div class="alert alert-success">
            <strong>✅ Success</strong><br>
            Strategy Consulting achieved 98% on-time delivery rate. 
            Recognized as preferred supplier!
        </div>
        """, unsafe_allow_html=True)

st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #718096; font-size: 0.85rem; margin-top: 2rem;">
    <p>🏢 ProcureIQ v2.0 | © 2024 All Rights Reserved</p>
    <p>Questions? Visit our <a href="#">documentation</a> or <a href="mailto:support@procureiq.local">contact support</a></p>
</div>
""", unsafe_allow_html=True)
