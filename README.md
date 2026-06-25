<div align="center">

# 📚 Book Analytics ETL Pipeline with SCD Type 2

**Production-Grade Veri Mühendisliği · Star Schema · Tarihsel Veri Takibi**

[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://mysql.com)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?style=for-the-badge&logo=python&logoColor=white)](https://sqlalchemy.org)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org)
[![YAML](https://img.shields.io/badge/YAML-Config-8A2BE2?style=for-the-badge&logo=yaml&logoColor=white)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

**Fail-Fast Validasyon · SHA-256 Hash ile SCD Type 2 · İdempotent Çalışma**

</div>

---

## 📌 Proje Hakkında

Bu proje, ham kitap satış verilerini işleyen, yerel formatları temizleyen ve **MySQL Veri Ambarına** yükleyen modüler bir **ETL Pipeline**'ıdır.

Basit veri taşıma scriptlerinden farklı olarak, **Veri Kalitesi** ve **Tarihsel Bütünlük** odaklıdır. **Slowly Changing Dimension (SCD) Type 2** stratejisi ile dimension attribute'lerindeki değişiklikleri izleyerek doğru tarihsel raporlama sağlar.

---

## 🏗️ Sistem Mimarisi

```
┌──────────┐    ┌───────────┐    ┌──────────────┐    ┌──────────┐
│   CSV    │ →  │  Extract   │ →  │  Transform   │ →  │   Load   │
│ (Ham Veri)│    │  & Clean   │    │  (SCD Hash)  │    │  (MySQL) │
└──────────┘    └───────────┘    └──────────────┘    └──────────┘
                     │                  │                   │
                     ↓                  ↓                   ↓
              Schema Validation    SHA-256 Hashing     SCD Type 2
              Type Enforcement    Business Logic      Idempotent Insert
              Format Temizleme                         Audit Log
```

---

## 🗂️ Veri Modeli (Star Schema)

```
📦 book_analytics_db (Star Schema)
│
├── 📊 fact_sales                        — Satış işlemleri (fact)
│   ├── book_id (FK)
│   ├── date_id (FK)
│   ├── sales_amount
│   └── quantity
│
├── 📋 dim_book (SCD Type 2)            — Kitap boyutu
│   ├── book_id (PK)
│   ├── title, author, genre
│   ├── price (zamanla değişebilir)
│   ├── is_active, start_date, end_date  ← SCD Type 2
│   └── hash_sha256                      ← Değişim algılama
│
├── 📋 dim_date                          — Tarih boyutu
│   └── date_id (PK), year, month, day
│
└── 📋 etl_logs                          — Pipeline audit tablosu
    └── run_id, status, duration, error
```

---

## ⚡ Temel Özellikler

### 🛡️ Robust Data Validation

Pipeline, yükleme öncesi sıkı veri kalitesi kuralları uygular:

| Kural | Açıklama |
|-------|----------|
| **Schema Validation** | Gerekli kolonların varlığını kontrol eder |
| **Type Enforcement** | `15.08.2019` → `YYYY-MM-DD`, `2.207,50` → `2207.50` |
| **Business Logic** | Negatif satış veya eksik business key içeren kayıtları reddeder |

### 🕰️ Slowly Changing Dimension (Type 2)

Veri evrimini tarihsel bütünlükle yönetir:

| Mekanizma | SHA-256 Hashing |
|-----------|----------------|
| **Çalışma Prensibi** | Her satır için takip edilen attributelardan hash üretilir |
| **Değişim Tespiti** | Hash mevcut kayıttan farklıysa → eski kayıt retire edilir (`is_active=0`, `end_date=NOW()`) |
| **Yeni Kayıt** | Güncel attributelar ile yeni kayıt insert edilir (`is_active=1`) |

### 🔍 İdempotency

Pipeline **idempotent** çalışır: Aynı veri seti birden çok kez çalıştırıldığında çift kayıt oluşmaz.

---

## ⚙️ Kurulum & Kullanım

```bash
# 1. Depoyu klonla
git clone https://github.com/ferhattkoc-ml/book-analytics-etl-scd2.git
cd book-analytics-etl-scd2

# 2. Sanal ortam oluştur
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows

# 3. Bağımlılıkları yükle
pip install -r requirements.txt

# 4. Konfigürasyon
cp config/db_config_example.yaml config/db_config.yaml
# Edit config/db_config.yaml with your MySQL credentials

# 5. Pipeline'ı çalıştır
python main.py
```

---

## 📊 Logging & Observability

Her çalıştırma `etl_logs` tablosuna kaydedilir.

| Alan | Açıklama |
|------|----------|
| `run_id` | Unique UUID — her batch için |
| `status` | `SUCCESS` veya `FAILED` |
| `duration_sec` | Execution time — performans takibi |
| `scd_inserts` | Yeni SCD history kaydı sayısı |
| `error_msg` | Hata durumunda detaylı stack trace |

---

## 🛠️ Tech Stack

| Kategori | Teknolojiler |
|----------|-------------|
| **Dil** | Python 3.8+ |
| **Veritabanı** | MySQL 8.0+ |
| **ORM/Driver** | SQLAlchemy, mysql-connector-python |
| **Veri İşleme** | Pandas |
| **Konfigürasyon** | PyYAML |
| **Loglama** | Python logging modülü |
| **Format Temizleme** | Regex (Türkçe/Almanca sayı formatları) |
| **Hash** | SHA-256 (hashlib) |

---

## 📂 Proje Yapısı

```
book-analytics-etl-scd2/
├── config/
│   └── db_config_example.yaml       # Örnek veritabanı konfigürasyonu
├── data/
│   ├── sql_data.csv                  # Ana satış verisi
│   ├── sql_data_dil.csv              # Dil referans verisi
│   ├── sql_data_kitap_adları.csv     # Kitap adı referans verisi
│   ├── sql_data_kitap_turu.csv       # Kitap türü referans verisi
│   └── sql_data_yazar_adlari.csv     # Yazar referans verisi
├── extract/
│   └── extract_mysql.py              # Extraction + temizleme katmanı
├── transform/
│   └── transform_book_analytics.py   # SCD hash + business logic
├── load/
│   ├── load_analytics.py             # Veri ambarı yükleme
│   └── load_scd2.py                  # SCD Type 2 insert/update
├── logs/
│   └── logger.py                     # Log konfigürasyonu
├── .gitignore
├── main.py                           # Pipeline entry point
├── requirements.txt                  # Bağımlılıklar
└── README.md                         # Bu dosya
```

---

## 🔮 Gelecek Planları

| Plan | Açıklama |
|------|----------|
| 🐳 **Containerization** | Dockerize ederek dağıtım kolaylığı |
| 🔄 **Orchestration** | Apache Airflow ile scheduling |
| 📈 **Visualization** | Metabase / Superset dashboard |
| 🧪 **Testing** | PyTest ile birim testleri |

---

## 🧠 Öğrenilen Kavramlar

- ✅ **ETL Pipeline Tasarımı** — Extract → Transform → Load
- ✅ **Slowly Changing Dimension Type 2** — SHA-256 hash ile değişim algılama
- ✅ **Star Schema** — Fact + Dimension modeli
- ✅ **İdempotency** — Tekrarlanabilir pipeline çalışması
- ✅ **Data Validation** — Schema, tip, iş kuralı kontrolleri
- ✅ **Fail-Fast** — Hatalı veriyi erken yakalama
- ✅ **Audit Logging** — Her batch için run_id bazlı izlenebilirlik

---

## 👤 Yazar

**Ferhat Koç** · [GitHub](https://github.com/ferhattkoc-ml) · [LinkedIn](https://linkedin.com/in/ferhattkocc/)

> ⭐ Bu projeyi beğendiyseniz bir yıldız bırakmayı unutmayın!

---

<div align="center">
  <sub>Built with ❤️ by Ferhat Koç</sub>
</div>
