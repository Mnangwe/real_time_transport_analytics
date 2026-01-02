import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

st.set_page_config(page_title="Transit Analytics", layout="wide")


@st.cache_resource
def get_engine():
    return create_engine(
        "postgresql://transit_user:transit_pass@localhost:5433/transit_db"
    )


@st.cache_data(ttl=300)
def load_metrics(days=7):

    engine = get_engine()

    query = f"""
        SELECT
            fm.hour,
            dtl.line_id,
            fm.avg_delay_seconds,
            fm.on_time_percentage,
            fm.total_arrivals,
            dd.full_date,
            dd.day_name
        FROM fact_hourly_metrics fm
        JOIN dim_transit_lines dtl
            ON fm.line_key = dtl.line_key
        JOIN dim_date dd
            ON fm.date_key = dd.date_key
        WHERE dd.full_date >= CURRENT_DATE - {days}
        ORDER BY dd.full_date, fm.hour
    """
    return pd.read_sql(query, engine)

st.title("🚍 Real-Time Transit Analytics Dashboard")

st.sidebar.header("Filters")
days = st.sidebar.slider("Days to analyze", 1, 30, 7)
df = load_metrics(days)

selected_lines = st.sidebar.multiselect(
    "Transit Lines",
    options=df['line_id'].unique(),
    default=df['line_id'].unique()[:5]
)

filtered_df = df[df['line_id'].isin(selected_lines)]

# Metrics row
col1, col2, col3, col4 = st.columns(4)

with col1:
    avg_delay = filtered_df['avg_delay_seconds'].mean()
    st.metric("Avg Delay", f"{avg_delay:.1f}s")

with col2:
    on_time = filtered_df['on_time_percentage'].mean()
    st.metric("On-Time %", f"{on_time:.1f}%")

with col3:
    total_trips = filtered_df['total_arrivals'].sum()
    st.metric("Total Trips", f"{total_trips:,.0f}")

with col4:
    lines_tracked = filtered_df['line_id'].nunique()
    st.metric("Lines Tracked", lines_tracked)

# Charts
col1, col2 = st.columns(2)

with col1:
    # Delay trends by hour
    hourly_avg = filtered_df.groupby('hour')['avg_delay_seconds'].mean().reset_index()
    fig1 = px.line(
        hourly_avg,
        x='hour',
        y='avg_delay_seconds',
        title='Average Delay by Hour of Day',
        labels={'hour': 'Hour', 'avg_delay_seconds': 'Delay (seconds)'}
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    # On-time performance by line
    line_performance = filtered_df.groupby('line_id')['on_time_percentage'].mean().reset_index()
    line_performance = line_performance.sort_values('on_time_percentage', ascending=True)

    fig2 = px.bar(
        line_performance,
        x='on_time_percentage',
        y='line_id',
        orientation='h',
        title='On-Time Performance by Line',
        labels={'on_time_percentage': 'On-Time %', 'line_id': 'Line'}
    )
    st.plotly_chart(fig2, use_container_width=True)


pivot_data = filtered_df.pivot_table(
    values='avg_delay_seconds',
    index='hour',
    columns='day_name',
    aggfunc='mean'
)

fig3 = go.Figure(data=go.Heatmap(
    z=pivot_data.values,
    x=pivot_data.columns,
    y=pivot_data.index,
    colorscale='RdYlGn_r'
))

fig3.update_layout(
    title='Delay Heatmap by Day and Hour',
    xaxis_title='Day of Week',
    yaxis_title='Hour'
)
st.plotly_chart(fig3, use_container_width=True)
