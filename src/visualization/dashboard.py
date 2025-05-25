import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import pydeck as pdk
import time

# Connexion à la base PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=15435,
    dbname="data_mart",
    user="postgres",
    password="admin"
)

st.set_page_config(layout="wide")
st.title("🚕 Dashboard Taxi NYC - Atelier Datamart")

# Récupération des données
@st.cache_data
def load_data():
    query = """
        SELECT f.Id_fact_trips, f.passenger_count, f.trip_distance, f.total_amount,
               f.fare_amount, f.tip_amount, f.tolls_amount,
               p.payment_description, r.ratecode_description, v.vendor_name,
               pl.zone AS pickup_zone, dl.zone AS dropoff_zone,
               pl.borough AS pickup_borough, dl.borough AS dropoff_borough,
               pl.service_zone AS pickup_service_zone, dl.service_zone AS dropoff_service_zone
        FROM fact_trips f
        JOIN dim_payment p ON f.payment_type = p.payment_type
        JOIN dim_ratecode r ON f.ratecodeid = r.ratecodeid
        JOIN dim_vendor v ON f.vendorid = v.vendorid
        JOIN dim_location pl ON f.pulocationid = pl.locationid
        JOIN dim_location dl ON f.dolocationid = dl.locationid
    """
    return pd.read_sql(query, conn)

df = load_data()

# Fonction d'animation de compteur avec décimales
def animated_number(label, end_value, format_str="{:.2f}", delay=0.01, steps=50):
    placeholder = st.empty()
    for i in range(steps):
        val = end_value * (i + 1) / steps
        placeholder.metric(label, format_str.format(val))
        time.sleep(delay)
    placeholder.metric(label, format_str.format(end_value))

# Animation d'intro
with st.spinner("Chargement des données et des visualisations..."):
    time.sleep(2)

# Vue synthétique
st.markdown("### Vue synthétique")
col1, col2, col3 = st.columns(3)
with col1:
    animated_number("Nombre total de courses", float(len(df)), format_str="{:.0f}")
with col2:
    animated_number("Montant total ($)", float(df['total_amount'].sum()), format_str="$ {:.2f}")
with col3:
    animated_number("Distance moyenne (mi)", float(df['trip_distance'].mean()), format_str="{:.2f} mi")

st.markdown("---")
st.markdown("### Graphiques principaux")

col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("Répartition des Paiements")
    fig1 = px.pie(df, names="payment_description", title="Répartition des Types de Paiement", hole=0.3)
    st.plotly_chart(fig1, use_container_width=True)
    time.sleep(0.2)

with col2:
    st.subheader("Répartition des Fournisseurs")
    fig2 = px.pie(df, names="vendor_name", title="Répartition des Fournisseurs", hole=0.3)
    st.plotly_chart(fig2, use_container_width=True)
    time.sleep(0.2)

with col3:
    st.subheader("Répartition des Passagers")
    fig3 = px.pie(df, names="passenger_count", title="Répartition du Nombre de Passagers", hole=0.3)
    st.plotly_chart(fig3, use_container_width=True)
    time.sleep(0.2)

col4, col5, col6 = st.columns(3)
with col4:
    st.subheader("Total par Zone d'Arrivée")
    fig4 = px.bar(
        df.groupby("dropoff_zone")["total_amount"].sum().nlargest(10).reset_index(),
        x="total_amount", y="dropoff_zone", orientation="h",
        labels={"total_amount": "Montant", "dropoff_zone": "Zone"},
        title="Top Zones d'Arrivée"
    )
    st.plotly_chart(fig4, use_container_width=True)
    time.sleep(0.2)

with col5:
    st.subheader("Distribution des Pourboires")
    fig5 = px.histogram(df, x="tip_amount", nbins=20, title="Distribution des Pourboires")
    st.plotly_chart(fig5, use_container_width=True)
    time.sleep(0.2)

with col6:
    st.subheader("Distance vs Montant Total")
    fig6 = px.scatter(
        df, x="trip_distance", y="total_amount",
        title="Distance vs Montant Total",
        labels={"trip_distance": "Distance (mi)", "total_amount": "Montant Total ($)"}
    )
    st.plotly_chart(fig6, use_container_width=True)
    time.sleep(0.2)

# Carte interactive des trajets
st.markdown("---")
st.subheader("🗺 Carte interactive des trajets")

# Utiliser les données de la base de données pour la carte
trip_data = df.copy()
trip_data['tooltip'] = trip_data.apply(
    lambda row: (
        f"De {row['pickup_zone']} à {row['dropoff_zone']}<br>"
        f"Passagers: {row['passenger_count']}<br>"
        f"Fournisseur: {row['vendor_name']}"
    ), axis=1
)

# Ajouter des coordonnées fictives pour la démonstration
trip_data['pickup_lat'] = 40.7128
trip_data['pickup_lon'] = -74.0060
trip_data['dropoff_lat'] = 40.6413
trip_data['dropoff_lon'] = -73.7781
trip_data['color'] = [[255, 0, 0]] * len(trip_data)

pickup_layer = pdk.Layer(
    "ScatterplotLayer", data=trip_data,
    get_position='[pickup_lon, pickup_lat]', get_fill_color='color',
    get_radius=200, pickable=True
)
dropoff_layer = pdk.Layer(
    "ScatterplotLayer", data=trip_data,
    get_position='[dropoff_lon, dropoff_lat]', get_fill_color='color',
    get_radius=200, pickable=True
)
line_layer = pdk.Layer(
    "LineLayer", data=trip_data,
    get_source_position='[pickup_lon, pickup_lat]',
    get_target_position='[dropoff_lon, dropoff_lat]',
    get_color='color', get_width=4
)
view_state = pdk.ViewState(latitude=40.7128, longitude=-74.0060, zoom=10, pitch=40)

st.pydeck_chart(pdk.Deck(
    layers=[pickup_layer, dropoff_layer, line_layer],
    initial_view_state=view_state,
    tooltip={"text": "{tooltip}"}
))

# Données brutes
st.markdown("---")
st.subheader("📃 Données brutes")
st.dataframe(df)

conn.close()
