import pandas as pd

def validate_inputs(data: dict) -> None:

    required_tables = [
        "fact_table",
        "dil",
        "kitap_adlari",
        "kitap_turleri",
        "yazar_adlari"
    ]

    for t in required_tables:
        if t not in data:
            raise ValueError(f"Missing table in extract output: {t}")

    fact_df = data["fact_table"]
    kitap_df = data["kitap_adlari"]

    # --- kitap_adlari control ---
    for col in ["kitap_id", "kitap_adi"]:
        if col not in kitap_df.columns:
            raise ValueError(f"kitap_adlari missing column: {col}")

    # --- fact_table control ---
    fact_required_cols = [
        "kitap_id",
        "yazar_id",
        "kitap_tur_id",
        "dil_id",
        "sayfa_sayisi",
        "yayin_tarihi",
        "satis_adedi",
        "satis_tutari"
    ]

    for col in fact_required_cols:
        if col not in fact_df.columns:
            raise ValueError(f"fact_table missing column: {col}")

    # --- Null Control ---
    not_null_cols = [
        "kitap_id",
        "yazar_id",
        "kitap_tur_id",
        "dil_id"
    ]

    for col in not_null_cols:
        if fact_df[col].isna().any():
            raise ValueError(f"fact_table.{col} contains NULL")

    

    #Uniqueness Control -> 1 çalışan 1 satır olmalı 


    duplicate_cols = ["kitap_id", "yayin_tarihi"]

    if fact_df.duplicated(subset=duplicate_cols).any():
        dup = fact_df.loc[
            fact_df.duplicated(subset=duplicate_cols),
            duplicate_cols
        ].head().to_dict(orient="records")

        raise ValueError(f"Duplicate fact records found: {dup}")


    if (fact_df["satis_adedi"] < 0).any():
        raise ValueError("fact_table.satis_adedi contains negative values")

    if (fact_df["satis_tutari"] < 0).any():
        raise ValueError("fact_table.satis_tutari contains negative values")


def transform_book_analytics(data: dict) -> pd.DataFrame:

    validate_inputs(data)

    fact_df = data["fact_table"].copy()
    kitap_df = data["kitap_adlari"].copy()
    yazar_df = data["yazar_adlari"].copy()
    tur_df = data["kitap_turleri"].copy()
    dil_df = data["dil"].copy()


    fact_df["yayin_tarihi"] = pd.to_datetime(
        fact_df["yayin_tarihi"], errors="coerce"
    )

    fact_df["satis_adedi"] = pd.to_numeric(
        fact_df["satis_adedi"], errors="coerce"
    )

    fact_df["satis_tutari"] = pd.to_numeric(
        fact_df["satis_tutari"], errors="coerce"
    )

    # Fail Fast
    if fact_df["yayin_tarihi"].isna().any():
        raise ValueError("fact_table.yayin_tarihi contains invalid values")

    if fact_df["satis_adedi"].isna().any():
        raise ValueError("fact_table.satis_adedi contains invalid values")

    if fact_df["satis_tutari"].isna().any():
        raise ValueError("fact_table.satis_tutari contains invalid values")


    fact_df["yil"] = fact_df["yayin_tarihi"].dt.year
    fact_df["ay"] = fact_df["yayin_tarihi"].dt.month

    # Ortalama satış fiyatı
    fact_df["birim_fiyat"] = (
        fact_df["satis_tutari"] / fact_df["satis_adedi"]
    )

    
    df = (
        fact_df
        .merge(kitap_df, on="kitap_id", how="inner")
        .merge(yazar_df, on="yazar_id", how="inner")
        .merge(tur_df, on="kitap_tur_id", how="inner")
        .merge(dil_df, on="dil_id", how="inner")
    )

    out = df[[
        "kitap_id",
        "kitap_adi",
        "yazar_adi",
        "kitap_tur_adi",
        "dil",
        "yayin_tarihi",
        "yil",
        "ay",
        "satis_adedi",
        "satis_tutari",
        "birim_fiyat"
    ]].copy()

    # Granularity kontrolü
    if out.duplicated(subset=["kitap_id", "yayin_tarihi"]).any():
        raise ValueError(
            "Output is not 1 row per kitap_id + yayin_tarihi"
        )

    return out
