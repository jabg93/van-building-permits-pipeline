# VAN Building Permits Pipeline

This project demonstrates a basic **Lakehouse-style architecture** for processing public open data using **Apache Airflow** and **dbt**, orchestrated and containerized via **Docker**. It focuses on showing how data can be ingested from an open API, stored in PostgreSQL, transformed using DBT, and prepared for analytics — all within a modular pipeline structure.

---

## Project Objective

To showcase how an orchestration tool like **Apache Airflow** can be combined with **dbt** for transforming raw data into analytics-ready models using the medallion architecture (bronze → silver → gold).

---

## Tech Stack

- **Docker**: Containerization and reproducibility
- **Apache Airflow**: Orchestration of data workflows
- **DBT**: Data transformation and modeling (ELT)
- **PostgreSQL**: Centralized database engine
- **AWS S3**: Object storage integration
- **Vancouver Open Data API**: Public data source

---

## Project Structure (2 levels map)
```bash
├── airflow
│   ├── dags
│   ├── data
│   ├── logs
│   └── plugins
├── dbt
│   ├── dbt_packages
│   ├── dbt_project.yml
│   ├── logs
│   ├── macros
│   ├── models
│   ├── package-lock.yml
│   ├── packages.yml
│   ├── profiles.yml
│   └── target
├── docker-compose.yaml
├── Dockerfile
├── LICENSE
├── README.md
└── requirements.txt
```

## Data Source

The source data comes from the **City of Vancouver's Open Data API**, specifically the dataset related to **Building Permits**.  
- API Endpoint: [https://opendata.vancouver.ca](https://opendata.vancouver.ca)

---

## Main Processes

- **Airflow DAG** pulls data from the public API and optionally uploads a copy to S3.
- Ingested data is stored in the **Bronze** layer of PostgreSQL.
- **DBT models** clean and transform this data into **Silver** and **Gold** layers.
- The final layer can be connected to BI tools (Power BI, Looker, etc.).

---

## How to Run

> **Note**: This was tested on a Mac with an M4 chip.

1. **Clone the repository** and ensure Docker & Docker Compose are installed.

```bash
git clone https://github.com/yourusername/van-building-permits-pipeline.git
cd van-building-permits-pipeline
```

2. **Create required folders** (If the project is cloned, it would include all required folders.)
```bash
mkdir -p airflow/{dags,logs,plugins}
```

3. **Initialize Airflow DB and create required users**
```bash
docker compose up airflow-init
```

4. **Start Airflow Services**
```bash 
docker compose up -d
```

5. **Verify that containers are running**
```bash 
docker compose ps
```

6. **Create Admin User**
```bash
docker exec -it van-building-permits-pipeline-webserver-1 bash

airflow users create \
  --username admin \
  --password admin \
  --firstname John \
  --lastname Smith \
  --role Admin \
  --email admin@example.org
```

7. **Access Airflow UI**
Visit http://localhost:8080

8. **Set Required Variables**
Via Airflow UI (Admin → Variables):

s3_bucket_name: your-s3-bucket
vancouver_api_url: full API endpoint URL

9. **Create Connections**
Use the Airflow UI to create:

A PostgreSQL connection
An AWS connection for S3

(Admin → Connections)

## Key Concepts
1. Use of Lakehouse medallion layers (bronze → silver → gold)
2. Scheduling and orchestration with Airflow
3. Scalable SQL transformations with DBT
4. Cloud-native design with Docker and S3 integration

## License
This project is released under the MIT License. Feel free to fork and adapt it for your own learning or production use.

## Credits
Developed by José Alberto Batista