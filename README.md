Book Analytics ETL Pipeline (SCD Type 2)

Production-style ETL pipeline built with Python, Pandas and MySQL, implementing full data validation, transformation, historical tracking (SCD Type 2) and run-level logging.

⸻

🚀 Project Purpose

This project simulates a real-world analytics ETL pipeline.

The main goals were:
	•	✅ Design a relational database model
	•	✅ Clean and normalize raw CSV data
	•	✅ Build a modular ETL architecture
	•	✅ Implement Slowly Changing Dimension (Type 2)
	•	✅ Add production-grade logging and monitoring
	•	✅ Follow real data engineering best practices

⸻

🏗 Database Design

The schema follows a simplified star-schema-like structure.

📌 Dimension Tables
	•	kitap_adlari
	•	yazar_adlari
	•	kitap_turleri
	•	dil

Each dimension table contains:
	•	PRIMARY KEY
	•	Descriptive attributes

⸻

📌 Fact Table

fact_table includes:
	•	fact_id (AUTO_INCREMENT, surrogate key)
	•	kitap_id (FK)
	•	yazar_id (FK)
	•	kitap_tur_id (FK)
	•	dil_id (FK)
	•	yayin_tarihi
	•	satis_adedi
	•	satis_tutari

Each row represents:

1 book + 1 publication date + sales metrics

🔐 Constraints Enforced
	•	PRIMARY KEY on dimensions
	•	FOREIGN KEY constraints
	•	DECIMAL(10,2) for monetary values
	•	ISO date format (YYYY-MM-DD)

⸻

🧹 Data Cleaning & Normalization

Raw CSV data initially contained:
	•	❌ Turkish date format (15.08.2019)
	•	❌ Thousand separators (2.207)
	•	❌ Decimal commas (30.765,58)

🔄 Transformations Applied
	•	Dates converted to ISO format
	•	Thousand separators removed
	•	Decimal commas converted to dot notation
	•	Numeric columns cast to correct data types
	•	Monetary values standardized

This ensured consistent and clean ingestion into MySQL.

