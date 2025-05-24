import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import squarify
from sqlalchemy import create_engine

# Configuration de la connexion PostgreSQL
engine = create_engine('postgresql://admin:admin@localhost:15435/data_mart')

# 1. Nombre de trajets par heure
query1 = """
SELECT dt.pickup_hour, COUNT(*) as nb_courses
FROM f_trips ft
JOIN dim_time dt ON ft.time_id = dt.id
GROUP BY dt.pickup_hour
ORDER BY dt.pickup_hour;
"""
df1 = pd.read_sql(query1, engine)

# 2. Total par mode de paiement
query2 = """
SELECT dp.payment_type, SUM(ft.total_amount) as total_amount
FROM f_trips ft
JOIN dim_payment dp ON ft.payment_id = dp.id
GROUP BY dp.payment_type
ORDER BY total_amount DESC;
"""
df2 = pd.read_sql(query2, engine)

# 3. Moyenne de passagers par vendor
query3 = """
SELECT dv.vendor_id, AVG(ft.passenger_count) as avg_passengers
FROM f_trips ft
JOIN dim_vendor dv ON ft.vendor_id = dv.id
GROUP BY dv.vendor_id;
"""
df3 = pd.read_sql(query3, engine)

# Affichage des graphes
sns.set(style="whitegrid")

# 1. Histogramme des trajets par heure
plt.figure(figsize=(10, 5))
sns.barplot(data=df1, x="pickup_hour", y="nb_courses", color="Blue")
plt.title("Nombre de trajets par heure")
plt.xlabel("Heure de prise en charge")
plt.ylabel("Nombre de trajets")
plt.tight_layout()
plt.show()

# Treemap : Total par mode de paiement
plt.figure(figsize=(10, 6))
colors = sns.color_palette("pastel", len(df2))
squarify.plot(
    sizes=df2['total_amount'], 
    label=df2['payment_type'], 
    color=colors, 
    alpha=0.8,
    text_kwargs={'fontsize': 12}
)
plt.title("Treemap des montants totaux par type de paiement")

# Création de la légende
handles = [plt.Rectangle((0,0),1,1, color=color) for color in colors]
plt.legend(handles, df2['payment_type'], title='Type de paiement', loc='upper right')

plt.axis('off')
plt.tight_layout()
plt.show()

# 3. Moyenne des passagers par Vendor
plt.figure(figsize=(8, 5))
sns.barplot(data=df3, x="vendor_id", y="avg_passengers", color="Green")
plt.title("Nombre moyen de passagers par Vendor")
plt.xlabel("Vendor ID")
plt.ylabel("Passagers moyens")
plt.tight_layout()
plt.show()
