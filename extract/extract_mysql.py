import pandas as pd
from sqlalchemy import create_engine
import yaml

with open("config/db_config.yaml", "r") as f:
    config = yaml.safe_load(f)

engine = create_engine(
    f'mysql+mysqlconnector://{config["user"]}:{config["password"]}@'
    f'{config["host"]}:{config["port"]}/{config["database"]}'
)

def extract_table(table_name):
    return pd.read_sql_table(table_name, engine)

def extract_all():
    return {
        "fact_table": extract_table("fact_table"),
        "dil": extract_table("dil"),
        "kitap_adlari": extract_table("kitap_adlari"),
        "kitap_turleri": extract_table("kitap_turleri"),
        "yazar_adlari": extract_table("yazar_adlari")
    }
