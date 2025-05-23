import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# ------------------- Config de la page -------------------
st.set_page_config(
    page_title="Dashboard Taxi NYC (Datamart)",
    page_icon="🚕",
    layout="wide"
)

# ------------------- Titre principal -------------------
st.title("Dashboard Datamart - Taxi NYC")
st.markdown("""
Bienvenue sur le tableau de bord exploratoire des données du Datamart (Fact + Dimensions).
Ce tableau de bord est limité à un échantillon de 10 000 courses pour des performances optimales.
""")

# ------------------- Connexion PostgreSQL (DataMart) -------------------
st.info("Connexion à la base de données Data Mart...")
engine = create_engine('postgresql+psycopg2://admin:admin@localhost:15435/nyc_datamart')

# Limiter les données pour éviter les lenteurs (on prend 10 000 courses seulement)
query = "SELECT * FROM fact_course ORDER BY id_course LIMIT 10000;"
df = pd.read_sql_query(query, engine)
st.success(f"{df.shape[0]} lignes chargées avec succès depuis le Data Mart !")

# ------------------- Sidebar - Filtres -------------------
st.sidebar.header("Filtres disponibles")

payment_options = sorted(df['payment_type_id'].dropna().unique().tolist())
selected_payment_type = st.sidebar.multiselect("Type de Paiement", payment_options, default=payment_options)

passenger_options = sorted(df['passenger_count_id'].dropna().unique().tolist())
selected_passengers = st.sidebar.multiselect("Nombre de passagers", passenger_options, default=passenger_options)

vendor_options = sorted(df['vendor_id'].dropna().unique().tolist())
selected_vendors = st.sidebar.multiselect("Vendor", vendor_options, default=vendor_options)

# Application des filtres
filtered_df = df[
    (df['payment_type_id'].isin(selected_payment_type)) &
    (df['passenger_count_id'].isin(selected_passengers)) &
    (df['vendor_id'].isin(selected_vendors))
]

st.markdown(f"### Nombre de courses affichées : {filtered_df.shape[0]:,}")

# ------------------- Statistiques globales -------------------
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Distance moyenne (miles)", f"{filtered_df['trip_distance'].mean():.2f}")

with col2:
    st.metric("Montant moyen ($)", f"{filtered_df['fare_amount'].mean():.2f}")

with col3:
    st.metric("Montant total ($)", f"{filtered_df['total_amount'].sum():,.2f}")

# ------------------- Visualisations -------------------

# 1. Histogramme des distances
st.subheader("Distribution des distances")
fig1 = px.histogram(filtered_df, x='trip_distance', nbins=50, title="Distances des courses (miles)")
st.plotly_chart(fig1, use_container_width=True)

# 2. Répartition des types de paiement
st.subheader("Répartition des types de paiement")
payment_counts = filtered_df['payment_type_id'].value_counts().reset_index()
payment_counts.columns = ['payment_type_id', 'count']
fig2 = px.pie(payment_counts, values='count', names='payment_type_id', title="Types de paiement")
st.plotly_chart(fig2, use_container_width=True)

# 3. Nombre de passagers
st.subheader("Répartition du nombre de passagers")
passenger_counts = filtered_df['passenger_count_id'].value_counts().reset_index()
passenger_counts.columns = ['passenger_count_id', 'count']
fig3 = px.bar(passenger_counts, x='passenger_count_id', y='count', title="Nombre de passagers par course")
st.plotly_chart(fig3, use_container_width=True)

# 4. Montant total par vendor
st.subheader("Montant total collecté par Vendor")
vendor_amount = filtered_df.groupby('vendor_id')['total_amount'].sum().reset_index()
fig4 = px.bar(vendor_amount, x='vendor_id', y='total_amount', title="Montant total par Vendor")
st.plotly_chart(fig4, use_container_width=True)

# ------------------- Fin -------------------
st.success("Dashboard Exploratoire terminé.")
st.markdown("""
---
Créé pour TP4 - Architecture Décisionnelle - Projet ATL Datamart 🚀
""")
