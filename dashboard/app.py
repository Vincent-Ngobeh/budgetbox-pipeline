"""
BudgetBox Analytics Dashboard

Interactive financial analytics built with Streamlit and Plotly.
Connects directly to the DuckDB data warehouse.
"""

import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Configuration
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/budgetbox.duckdb")

st.set_page_config(
    page_title="BudgetBox Analytics",
    page_icon="📊",
    layout="wide",
)


@st.cache_resource
def get_connection():
    """Get DuckDB connection."""
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@st.cache_data(ttl=60)
def load_transactions():
    """Load fact transactions with dimensions."""
    conn = get_connection()
    query = """
        SELECT 
            f.transaction_id,
            f.transaction_date,
            f.amount,
            f.signed_amount,
            f.amount_gbp,
            f.signed_amount_gbp,
            f.transaction_type,
            f.merchant,
            f.description,
            a.account_name,
            a.account_type,
            c.category_name,
            c.parent_category,
            c.category_type,
            d.day_name,
            d.month_name,
            d.year_number
        FROM main_marts.fact_transactions f
        LEFT JOIN main_marts.dim_accounts a ON f.account_key = a.account_key
        LEFT JOIN main_marts.dim_categories c ON f.category_key = c.category_key
        LEFT JOIN main_marts.dim_dates d ON f.date_key = d.date_key
        ORDER BY f.transaction_date DESC
    """
    return conn.execute(query).fetchdf()


@st.cache_data(ttl=60)
def load_summary_stats():
    """Load summary statistics."""
    conn = get_connection()
    query = """
        SELECT
            COUNT(*) as total_transactions,
            SUM(CASE WHEN transaction_type = 'credit' THEN amount_gbp ELSE 0 END) as total_income,
            SUM(CASE WHEN transaction_type = 'debit' THEN amount_gbp ELSE 0 END) as total_expenses,
            COUNT(DISTINCT merchant) as unique_merchants
        FROM main_marts.fact_transactions
    """
    return conn.execute(query).fetchdf().iloc[0]


def render_header():
    """Render dashboard header."""
    st.title("BudgetBox Analytics Dashboard")
    st.markdown("Real-time financial insights from your data warehouse")
    st.divider()


def render_kpi_cards(stats):
    """Render KPI metric cards."""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Transactions",
            value=f"{stats['total_transactions']:,}",
        )
    
    with col2:
        st.metric(
            label="Total Income",
            value=f"£{stats['total_income']:,.2f}",
        )
    
    with col3:
        st.metric(
            label="Total Expenses", 
            value=f"£{stats['total_expenses']:,.2f}",
        )
    
    with col4:
        net = stats['total_income'] - stats['total_expenses']
        st.metric(
            label="Net Position",
            value=f"£{net:,.2f}",
            delta=f"{'Surplus' if net > 0 else 'Deficit'}",
        )


def render_spending_by_category(df):
    """Render spending by category chart."""
    st.subheader("Spending by Category")
    
    expenses = df[df['transaction_type'] == 'debit'].copy()
    if expenses.empty:
        st.info("No expense data available")
        return
        
    category_totals = expenses.groupby('category_name')['amount_gbp'].sum().reset_index()
    category_totals = category_totals.sort_values('amount_gbp', ascending=True)
    
    fig = px.bar(
        category_totals,
        x='amount_gbp',
        y='category_name',
        orientation='h',
        labels={'amount_gbp': 'Amount (GBP)', 'category_name': 'Category'},
        color='amount_gbp',
        color_continuous_scale='Reds',
    )
    fig.update_layout(
        showlegend=False,
        height=400,
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_spending_over_time(df):
    """Render spending over time chart."""
    st.subheader("Transactions Over Time")
    
    daily = df.groupby(['transaction_date', 'transaction_type'])['amount_gbp'].sum().reset_index()
    
    fig = px.line(
        daily,
        x='transaction_date',
        y='amount_gbp',
        color='transaction_type',
        labels={'amount_gbp': 'Amount (GBP)', 'transaction_date': 'Date'},
        color_discrete_map={'credit': '#2ecc71', 'debit': '#e74c3c'},
    )
    fig.update_layout(
        height=350,
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_top_merchants(df):
    """Render top merchants table."""
    st.subheader("Top Merchants by Spend")
    
    expenses = df[df['transaction_type'] == 'debit'].copy()
    if expenses.empty:
        st.info("No expense data available")
        return
        
    merchant_totals = expenses.groupby('merchant').agg(
        total_spend=('amount_gbp', 'sum'),
        transaction_count=('transaction_id', 'count'),
    ).reset_index()
    merchant_totals = merchant_totals.sort_values('total_spend', ascending=False).head(10)
    merchant_totals['total_spend'] = merchant_totals['total_spend'].apply(lambda x: f"£{x:,.2f}")
    merchant_totals.columns = ['Merchant', 'Total Spend', 'Transactions']
    
    st.dataframe(merchant_totals, use_container_width=True, hide_index=True)


def render_category_breakdown(df):
    """Render category breakdown pie chart."""
    st.subheader("Category Breakdown")
    
    expenses = df[df['transaction_type'] == 'debit'].copy()
    if expenses.empty:
        st.info("No expense data available")
        return
    
    parent_totals = expenses.groupby('parent_category')['amount_gbp'].sum().reset_index()
    
    fig = px.pie(
        parent_totals,
        values='amount_gbp',
        names='parent_category',
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(
        height=350,
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_recent_transactions(df):
    """Render recent transactions table."""
    st.subheader("Recent Transactions")
    
    recent = df.head(20)[['transaction_date', 'merchant', 'category_name', 'amount_gbp', 'transaction_type']].copy()
    recent['amount_gbp'] = recent.apply(
        lambda r: f"+£{r['amount_gbp']:,.2f}" if r['transaction_type'] == 'credit' else f"-£{r['amount_gbp']:,.2f}",
        axis=1
    )
    recent = recent.drop(columns=['transaction_type'])
    recent.columns = ['Date', 'Merchant', 'Category', 'Amount']
    
    st.dataframe(recent, use_container_width=True, hide_index=True)


def render_day_of_week_analysis(df):
    """Render spending by day of week."""
    st.subheader("Spending by Day of Week")
    
    expenses = df[df['transaction_type'] == 'debit'].copy()
    if expenses.empty:
        st.info("No expense data available")
        return
    
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_totals = expenses.groupby('day_name')['amount_gbp'].sum().reset_index()
    day_totals['day_name'] = pd.Categorical(day_totals['day_name'], categories=day_order, ordered=True)
    day_totals = day_totals.sort_values('day_name')
    
    fig = px.bar(
        day_totals,
        x='day_name',
        y='amount_gbp',
        labels={'amount_gbp': 'Amount (GBP)', 'day_name': 'Day'},
        color='amount_gbp',
        color_continuous_scale='Blues',
    )
    fig.update_layout(
        showlegend=False,
        height=300,
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)


def main():
    """Main dashboard entry point."""
    render_header()
    
    try:
        # Load data
        df = load_transactions()
        stats = load_summary_stats()
        
        if df.empty:
            st.warning("No transaction data found. Run the ingestion pipeline first.")
            return
        
        # KPI Cards
        render_kpi_cards(stats)
        st.divider()
        
        # Row 1: Spending over time
        render_spending_over_time(df)
        
        # Row 2: Two columns
        col1, col2 = st.columns(2)
        with col1:
            render_spending_by_category(df)
        with col2:
            render_category_breakdown(df)
        
        st.divider()
        
        # Row 3: Two columns
        col1, col2 = st.columns(2)
        with col1:
            render_top_merchants(df)
        with col2:
            render_day_of_week_analysis(df)
        
        st.divider()
        
        # Row 4: Recent transactions
        render_recent_transactions(df)
        
        # Footer
        st.divider()
        st.caption(f"Data refreshed from DuckDB warehouse. Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.info("Make sure the dbt models have been run: `cd dbt/budgetbox && dbt run`")


if __name__ == "__main__":
    main()
