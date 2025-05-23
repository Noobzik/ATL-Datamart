import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# Configuration
DB_USER = "admin"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "15435"
DB_NAME = "nyc_datamart"

# Database connection using SQLAlchemy
@st.cache_resource
def get_engine():
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

engine = get_engine()

@st.cache_data
def load_data():
    query = """
    SELECT ft.*, dv.vendor_name, drc.rate_description, dpt.payment_description,
           dl1.location_description AS pickup_desc,
           dl2.location_description AS dropoff_desc,
           dd.pickup_datetime, dd.dropoff_datetime
    FROM fact_trips ft
    LEFT JOIN dim_vendor dv ON ft.vendor_id = dv.vendor_id
    LEFT JOIN dim_rate_code drc ON ft.rate_code_id = drc.rate_code_id
    LEFT JOIN dim_payment_type dpt ON ft.payment_type_id = dpt.payment_type_id
    LEFT JOIN dim_location dl1 ON ft.pickup_location_id = dl1.location_id
    LEFT JOIN dim_location dl2 ON ft.dropoff_location_id = dl2.location_id
    LEFT JOIN dim_datetime dd ON ft.datetime_id = dd.datetime_id
    """
    return pd.read_sql(query, engine)

df = load_data()

st.title("NYC Taxi Trip Analytics Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
date_range = st.sidebar.date_input("Date range", [])
if date_range and len(date_range) == 2:
    df = df[(df["pickup_datetime"] >= pd.to_datetime(date_range[0])) &
            (df["pickup_datetime"] <= pd.to_datetime(date_range[1]))]

# 1. Trip volume over time
st.subheader("Trip Volume Over Time")
volume_df = df.groupby(pd.to_datetime(df['pickup_datetime']).dt.date).size().reset_index(name='Trips')
fig1 = px.line(volume_df, x='pickup_datetime', y='Trips', title='Daily Trip Volume')
st.plotly_chart(fig1, use_container_width=True)

# 2. Trips by vendor
st.subheader("Trips by Vendor")
fig2 = px.pie(df, names='vendor_name', title='Distribution of Trips by Vendor')
st.plotly_chart(fig2, use_container_width=True)

# 3. Trips by rate code
st.subheader("Trips by Rate Code")
counts = df['rate_description'].value_counts().reset_index()
counts.columns = ['rate_description', 'count']

fig3 = px.bar(counts, x='rate_description', y='count')
st.plotly_chart(fig3, use_container_width=True)

# 4. Trips by payment type
st.subheader("Trips by Payment Type")
counts = df['payment_description'].value_counts().reset_index()
counts.columns = ['payment_description', 'count']

fig4 = px.bar(counts, x='payment_description', y='count')
st.plotly_chart(fig4, use_container_width=True)

# 5. Trip distance histogram
st.subheader("Trip Distance Distribution")
fig5 = px.histogram(df, x='trip_distance', nbins=50, title='Trip Distance Distribution')
st.plotly_chart(fig5, use_container_width=True)

# 6. Average fare by pickup & dropoff
st.subheader("Average Fare by Pickup & Dropoff")
fare_loc = df.groupby(['pickup_desc', 'dropoff_desc'])['fare_amount'].mean().reset_index()
fare_loc = fare_loc.sort_values('fare_amount', ascending=False).head(20)
fig6 = px.bar(fare_loc, x='fare_amount', y='pickup_desc', color='dropoff_desc',
              orientation='h', title='Top 20 Avg Fares by Pickup/Dropoff')
st.plotly_chart(fig6, use_container_width=True)

# 7. Tip vs fare scatter
st.subheader("Tip Amount vs Fare Amount")
fig7 = px.scatter(df, x='fare_amount', y='tip_amount', title='Tip vs Fare Amount', trendline='ols')
st.plotly_chart(fig7, use_container_width=True)

# 8. Heatmap by hour and day
st.subheader("Trip Frequency Heatmap by Hour and Day")
df['hour'] = pd.to_datetime(df['pickup_datetime']).dt.hour
df['day'] = pd.to_datetime(df['pickup_datetime']).dt.day_name()
heatmap_data = df.groupby(['day', 'hour']).size().reset_index(name='Trips')
heatmap_pivot = heatmap_data.pivot(index='day', columns='hour', values='Trips').fillna(0)
fig8 = go.Figure(data=go.Heatmap(
    z=heatmap_pivot.values,
    x=heatmap_pivot.columns,
    y=heatmap_pivot.index,
    colorscale='Viridis'))
fig8.update_layout(title='Trips by Hour and Day of Week')
st.plotly_chart(fig8, use_container_width=True)

# 9. Revenue components
st.subheader("Total Revenue Breakdown")
components = ['fare_amount', 'extra', 'mta_tax', 'tip_amount',
              'tolls_amount', 'improvement_surcharge', 'congestion_surcharge', 'airport_fee']
rev_df = df[components].sum().reset_index()
rev_df.columns = ['Component', 'Total Amount']
fig9 = px.bar(rev_df, x='Component', y='Total Amount', title='Revenue Breakdown')
st.plotly_chart(fig9, use_container_width=True)
