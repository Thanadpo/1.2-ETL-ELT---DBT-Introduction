# 1.2-ETL-ELT---DBT-Introduction

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from urllib.parse import quote_plus

# Paramètres de connexion.
username = "root"
password = quote_plus("Formation2thomas!")
host = "localhost"
port = 3306
database = "my_dbt_db"

# URI de connexion.
DATABASE_URI = f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(DATABASE_URI)

# Créer la base de données si elle n'existe pas.
if not database_exists(engine.url):
    create_database(engine.url)

# Charger les données des fichiers CSV.
liste_tables = ["customers", "items", "orders", "products", "stores", "supplies"]

for table in liste_tables:
    csv_url = f"https://raw.githubusercontent.com/dsteddy/jaffle_shop_data/main/raw_{table}.csv"
    df = pd.read_csv(csv_url)
    df.to_sql(f"raw_{table}", engine, if_exists="replace", index=False)
