# Real-Time Data Pipeline with Kafka, Airflow and Postgres

## 📌 Project Overview 
This project implements an end-to-end **data engineering pipeline** that ingests data from an external API in real time using **Apache Kafka**, stages the data in **PostgreSQL**, and processes it into **dimension and fact tables** using **Apache Airflow**.

The pipeline is containerized using **Docker** and includes **data quality testing with Pytest** and **data visualization using Streamlit**.

The primary purpose of the project is to demonstrate a scalable, production-style architecture for streaming ingestion, batch processing, dimensional modeling, and analytics.

## 🏗️ Architecture Overview
### High-level flow:
1. Externa API ( [https://api.tfl.gov.uk/](https://api.tfl.gov.uk/) )
2. Kafka Producer ( data ingestion )
3. Kafka Broker & Zookeeper ( Confluent images )
4. Kafka Consumer
5. PostgreSQL Staging Tables
6. Airflow DAG
7. Dimension Tables (Type 1 & Type 2)
8. Fact Tables
9. Pytest Data Quality Checks
10. Streamlit Dashboard

## 🛠️ Technology Stack

| Layer            | Tools                           |
| ---------------- | ------------------------------- |
| Streaming        | Apache Kafka (Confluent Images) |
| Orchestration    | Apache Airflow                  |
| Database         | PostgreSQL                      |
| Testing          | Pytest                          |
| Visualization    | Streamlit (Choose any tool)     |
| Containerization | Docker & Docker Compose         |
| Language         | Python                          |


## 📦 Project Components
### Kafka Producer
* Fetches data from an **external API**
* Publishes raw events to **Kafka topics**
* Ensures decoupled and **scalable** ingestion
### Kafka Consumer
* Subscribes to Kafka topics
* Writes incoming messages to **Postgres staging tables**
* Minimal transformation at this stage (staging layer)

## 🗄️ Data Modeling
### Staging Layer
* Stores raw ingested data from Kafka
* Acts as a source of truth for downstream processing
### Dimensions Tables
* #### Type 1 Dimension 
    * Overwrites existing records
    * Used for non-historical attributes
* #### Type 2 Dimension
    * Preserves historical changes
    * Uses effective dates and current flags
### Fact Tables
* Populated using **upsert logic**
* References dimension **surrogate keys**
* Built for **analytical** querying

## ⏱️ Airflow Orchestration
Airflow DAG is used to:
* Read data from staging tables
* Perform upserts into dimension tables
* Load fact tables
* Enforce task dependecies and retry logic

## ✅ Data Quality & Testing
**Pytest** is used to validate:
* Staging table not empty
* Uniqueness constraints
* Null value checks
* Foreign keys on Fact tables
* Current Records (Type 2 Dims)
* Expected value ranges
* Record counts
* Business metric sanity checks

These tests ensure the pipeline produces **reliable and trustworthy data**.

## 📊 Visualization
**Streamlit** is used to:
* Query the final fact and dimension tables
* Display key metrics and trends
* Provide a lightweight analytics dashboard

## 🐳 Dockerized Setup
All services run in Docker containers:
* Kafka (Zookeeper & Broker via Confluent images)
* Airflow (Scheduler, Webserver)
* PostgreSQL
* Adminer (Database IDE)

This ensures:
* Environment consistency
* Easy local setup
* Reproducibility

## ▶️ Running the Project
```bash
# Start all services
docker compose up -d

# Install local requirements
pip install -r requirements.txt

# Data Ingest
# Run Producer local script
python ./src/producers/tfl_producer.py

# Run Consumer local script
python ./src/producers/tfl_consumer.py

# Access Airflow UI
http://localhost:8080

# Access Postgres UI
http://localhost:8081

# Testing
pytest -m ./tests/test_data_quality.py -v

# Run streamlit local
streamlit run ./dashboard/app.py
```

## 📁 Project Structure
```text
.
├── scripts/
│   └── create_tables.sql
├── src/
│   ├── consumers/
│   │   └── tfl_consumer.py
│   ├── dags/
│   │   └── transit_etl_dag.py
│   ├── dashboard/
│   │   └── app.py
│   ├── producers/
│   │   └── tfl_producer.py
│   ├── tests/
│   │   ├── conftest.py
│   │   ├── test_api.py
│   │   └── test_data_quality.py
│   └── utils/
├── .env
├── .gitignore
├── docker-compose.yml
├── Makefile
├── README.md
└── requirements.txt
```

## 🚀 Future Improvements
* Introduce transformations using Spark
* Improve monitoring and alerting
* Implement incremental loads
* Add more grain on the data (More *store_id*)

## 👤 Author
#### Azabenathi Pupuma