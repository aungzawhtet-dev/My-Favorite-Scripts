# My-Favorite-Scripts
This repository showcases a production-oriented ETL pipeline orchestrated with Apache Airflow, extracting data from MongoDB, transforming it with Pandas, and loading it into PostgreSQL with data quality checks and reconciliation.
The project focuses on real-world data engineering challenges: NoSQL-to-relational normalization, backfill-safe scheduling, idempotent loading, and operational reliability.

I particularly like this pipeline because it demonstrates practical data engineering fundamentals: working with NoSQL to relational data, schema alignment, anonymized and reusable design, and production-ready practices such as environment variables, idempotent table creation, and clear DAG structure. It reflects how I approach building maintainable, readable pipelines that can be extended with incremental loading, monitoring, and scaling when needed.

# Architecture
<img width="1024" height="1536" alt="Architecture" src="https://github.com/user-attachments/assets/575733eb-56b1-41bf-8bae-419b305a6c74" />


