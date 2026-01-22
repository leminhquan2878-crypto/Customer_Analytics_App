# 📊 Customer 360 & Content Analytics Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9-blue)
![PySpark](https://img.shields.io/badge/Apache%20Spark-ETL-orange)
![Docker](https://img.shields.io/badge/Docker-Container-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-336791)
![Power BI](https://img.shields.io/badge/Power%20BI-Visualization-f2c811)

## 📖 Project Overview

This project is an **End-to-End Data Engineering Pipeline** designed to process and analyze user interaction logs (Watch History & Search History) for a media streaming platform.

The system ingests raw "dirty" logs, transforms them using **PySpark** (handling complex data quality issues like Thai Buddhist dates), loads them into a **PostgreSQL Data Warehouse** modeled with **Star Schema**, and visualizes insights via a **Power BI** dashboard.


## 📂 Project Structure

```bash
Customer_Analytics_App/
│
├── config/                     
│   └── postgresql-42.7.2.jar  
│
├── data/                       
│   └── raw/
│       ├── log_content/        
│       └── log_search/        
│
├── sql/                        
│   └── init_schema.sql         
│
├── src/                       
│   ├── __init__.py
│   └── etl_job.py           
│
├── reports/                  
│   └── Customer_360_Insights.pbix
│
├── images/                    
│   └── dashboard_demo.png
│
├── docker-compose.yml         
├── Dockerfile                  
├── requirements.txt            
└── README.md                   
```
## 🔄 Full Pipeline Flow

The data flows sequentially through the following stages:

1.  **Ingestion (Data Lake):**
    * Raw user logs (`log_search.json`, `log_content.csv`) are stored in the local `data/raw/` directory.
    * These files are mounted into the **PySpark** container via Docker Volumes.

2.  **ETL Processing (PySpark):**
    * Spark reads the raw data and performs data cleaning (handling **Thai Buddhist dates**, normalizing numerals, removing NULLs).
    * Data is transformed and mapped to the **Star Schema** logic (creating Fact and Dimension dataframes).

3.  **Data Warehousing (PostgreSQL):**
    * The transformed data is written into **PostgreSQL** tables using the JDBC driver.
    * Data is organized into Fact tables (`fact_watch`, `fact_search`) and Dimension tables (`dim_customer`, `dim_date`).

4.  **Reporting & Visualization (Power BI):**
    * **Power BI** connects directly to the PostgreSQL container (via localhost:5432).
    * The dashboard visualizes key metrics such as User Segmentation, Search Trends, and Content Popularity.
## 🚀 Key Features & Technical Highlights

### 1. Complex Data Cleaning (PySpark)

- **Thai Date Conversion:** Successfully handled logs using the Thai Buddhist Calendar (e.g., converting year `2565` to `2022`).
- **Numeral Normalization:** Converted non-standard numerals (Persian, Bengali) into standard integers.
- **Data Integrity:** Filtered out logs with NULL keys to ensure Referential Integrity in the Data Warehouse.

### 2. Star Schema Modeling (PostgreSQL)

Designed a dimensional model optimized for analytical queries:

- **Fact Tables:** `fact_watch_activity`, `fact_search_activity`
- **Dimension Tables:** `dim_customer`, `dim_content_type`, `dim_date`, `dim_keyword`
- **Automation:** The `dim_date` table is auto-populated with 10 years of data using SQL `GENERATE_SERIES`.

### 3. Containerization (Docker)

- Used **Docker Compose** to orchestrate Spark and PostgreSQL services.
- Implemented **Volume Mapping** to handle data ingestion without copying files into containers.
- configured **Auto-Initialization** for the database using `docker-entrypoint-initdb.d`.

## 🛠️ How to Run

### Prerequisites

- Docker Desktop installed.
- Power BI Desktop (for viewing reports).

### Step 1: Setup Data & Config

Ensure you have the JDBC driver in the `config/` folder and raw data in `data/raw/`.

### Step 2: Start the Pipeline

Open your terminal in the project root and run:

```bash
docker-compose up --build
```

_This command will start PostgreSQL, initialize the tables, and trigger the PySpark ETL job automatically._

### Step 3: Verify Data

Connect to PostgreSQL using any client (DBeaver, pgAdmin):

- **Host:** `localhost`
- **Port:** `5432`
- **Database:** `customer_dw`
- **User/Pass:** `postgres` / `password`

### Step 4: Visualize

Open `reports/Customer_360_Insights.pbix` in Power BI to view the dashboard.

## 📊 Dashboard Demo

_(Screenshot of the Power BI Dashboard)_

![Dashboard Demo](images/dashboard_demo.png)
