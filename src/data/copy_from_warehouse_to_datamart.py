import pandas as pd
from sqlalchemy import create_engine

# Connexion au Data Warehouse (source)
engine_wh = create_engine('postgresql://admin:admin@127.0.0.1:15432/nyc_warehouse')
df = pd.read_sql('SELECT * FROM nyc_raw', engine_wh)

print(f"✅ Données récupérées depuis le Data Warehouse: {len(df)} lignes")

# Connexion au Data Mart (destination)
engine_dm = create_engine('postgresql://admin:admin@127.0.0.1:15435/nyc_datamart')

# Insertion dans nyc_raw du Data Mart (on remplace s'il y avait)
df.to_sql('nyc_raw', engine_dm, if_exists='replace', index=False)

print("✅ Copie des données dans le Data Mart terminée avec succès.")
