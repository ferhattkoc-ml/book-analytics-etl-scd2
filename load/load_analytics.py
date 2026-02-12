import yaml
from sqlalchemy import create_engine

def get_engine():
    with open("config/db_config.yaml", "r") as f:
        config = yaml.safe_load(f)

    return create_engine(
        f'mysql+mysqlconnector://{config["user"]}:{config["password"]}@'
        f'{config["host"]}:{config["port"]}/{config["database"]}'
    )

# Full Replace Load
def full_replace_table(df, table_name: str = "analytics_books"):

    engine = get_engine()

    df.to_sql(
        table_name,
        engine,
        if_exists="replace",   # tabloyu silip yeniden oluşturur
        index=False
    )
