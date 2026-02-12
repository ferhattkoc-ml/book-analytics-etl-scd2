import pandas as pd
import hashlib
import yaml
from sqlalchemy import create_engine, text, bindparam

SCD_TABLE = "book_analytics_scd2"


# -------------------------------------------------
# Engine
# -------------------------------------------------
def get_engine():
    with open("config/db_config.yaml", "r") as f:
        config = yaml.safe_load(f)

    return create_engine(
        f'mysql+mysqlconnector://{config["user"]}:{config["password"]}@'
        f'{config["host"]}:{config["port"]}/{config["database"]}'
    )


# -------------------------------------------------
# Hash üretimi (kitap için)
# -------------------------------------------------
def compute_hash_row(row: pd.Series) -> str:

    tracked = [
        "kitap_adi",
        "yazar_adi",
        "kitap_tur_adi",
        "dil",
        "birim_fiyat"
    ]

    s = "||".join(
        "" if pd.isna(row.get(c)) else str(row.get(c))
        for c in tracked
    )

    return hashlib.md5(s.encode("utf-8")).hexdigest()


# -------------------------------------------------
# Tablo oluşturma
# -------------------------------------------------
def ensure_table_exists(engine):

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SCD_TABLE} (
        scd_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        kitap_id INT NOT NULL,
        kitap_adi VARCHAR(150),
        yazar_adi VARCHAR(150),
        kitap_tur_adi VARCHAR(100),
        dil VARCHAR(50),
        birim_fiyat DECIMAL(12,2),
        record_hash CHAR(32) NOT NULL,
        effective_from DATETIME NOT NULL,
        effective_to DATETIME NULL,
        is_current TINYINT(1) NOT NULL DEFAULT 1,
        INDEX idx_book_current (kitap_id, is_current),
        INDEX idx_book_from (effective_from)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))


# -------------------------------------------------
# SCD2 UPSERT
# -------------------------------------------------
def scd2_upsert(df: pd.DataFrame):

    engine = get_engine()
    ensure_table_exists(engine)

    run_ts = pd.Timestamp.now().to_pydatetime()

    snap = df.copy()

    snap["record_hash"] = snap.apply(compute_hash_row, axis=1)
    snap["effective_from"] = run_ts
    snap["effective_to"] = None
    snap["is_current"] = 1

    # Mevcut aktif kayıtları al
    current_sql = f"""
        SELECT kitap_id, record_hash
        FROM {SCD_TABLE}
        WHERE is_current = 1
    """
    current = pd.read_sql(current_sql, engine)

    merged = snap.merge(
        current,
        on="kitap_id",
        how="left",
        suffixes=("", "_cur")
    )

    new_mask = merged["record_hash_cur"].isna()

    changed_mask = (
        (~new_mask) &
        (merged["record_hash"] != merged["record_hash_cur"])
    )

    new_rows = merged.loc[new_mask, snap.columns].copy()
    changed_rows = merged.loc[changed_mask, snap.columns].copy()

    with engine.begin() as conn:

        # Değişenleri kapat
        if not changed_rows.empty:

            changed_ids = (
                changed_rows["kitap_id"]
                .astype(int)
                .unique()
                .tolist()
            )

            stmt = text(f"""
                UPDATE {SCD_TABLE}
                SET effective_to = :run_ts,
                    is_current = 0
                WHERE is_current = 1
                  AND kitap_id IN :ids
            """).bindparams(
                bindparam("ids", expanding=True)
            )

            conn.execute(stmt, {
                "run_ts": run_ts,
                "ids": changed_ids
            })

        to_insert = pd.concat(
            [new_rows, changed_rows],
            ignore_index=True
        )

        if not to_insert.empty:

            to_insert = to_insert[[
                "kitap_id",
                "kitap_adi",
                "yazar_adi",
                "kitap_tur_adi",
                "dil",
                "birim_fiyat",
                "record_hash",
                "effective_from",
                "effective_to",
                "is_current"
            ]]

            to_insert.to_sql(
                SCD_TABLE,
                conn,
                if_exists="append",
                index=False
            )

    return {
        "run_ts": run_ts,
        "new_count": int(new_rows.shape[0]),
        "changed_count": int(changed_rows.shape[0]),
        "inserted": int(new_rows.shape[0] + changed_rows.shape[0])
    }