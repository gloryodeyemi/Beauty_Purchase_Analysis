# 🛠️  End-to-End Data Pipeline for Beauty Products Purchase Analysis
An end-to-end data engineering and analysis project that builds an automated data pipeline from Google Sheets to Snowflake, orchestrated with Apache Airflow and visualized in Tableau. This project captures real-world personal beauty product purchases, transforming raw entries into structured insights.

## 📌 Project Overview
This project analyzes real-world beauty product purchase data through a robust data pipeline and visualization framework. It was designed to:
* Track personal purchase behaviors and product preferences.
* Uncover trends in spending, product types, and brand loyalty.
* Serve as a template for others to understand purchase patterns using real-life data and modern analytics tools.

## 👩‍💻 Tech Stack
| Tool | Purpose |
|------|---------|
| ![Google Sheets](https://img.shields.io/badge/Google%20Sheets-34A853?style=for-the-badge&logo=googlesheets&logoColor=white) | Raw data source – purchase entries are logged here |
| ![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white) | Data extraction, cleaning, transformation, and Snowflake loading |
| ![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white) | Data warehouse – stores fact & dimension tables |
| ![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white) | Pipeline automation & scheduling (weekly refresh) |
| ![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white) | Containerization for Apache Airflow services |
| ![Tableau](https://img.shields.io/badge/Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white) | Final data visualization and dashboard exploration |

## 🔄 ELT Pipeline Architecture
### 1. Extract
  * Source: Google Sheets with manually logged purchase data.
  * Tool: Python uses gspread and pandas to pull the data.

### 2. Transform
  * Initial transformations: Removing duplicates, handling missing values, data type conversion, data validation, and standardization, joining, and creating new columns.
  * Tool: Python pandas library.

### 3. Load
  * The transformed data is loaded into a raw schema/table in Snowflake
  * Tool: Snowflake Python Connector.

### 4. Further Transformations
  * Data modeling: The fact and dimension tables are designed in Snowflake using the Snowflake schema.
  * Records are inserted into the appropriate fact and dimension tables using a stored procedure.
  * Tool: Snowflake SQL.

### 5. Orchestration
  * Apache Airflow DAG runs the full pipeline weekly.
  * Handles error logging and ensures consistent data refresh.
    
  ![img](https://github.com/gloryodeyemi/Beauty_Purchase_Analysis/blob/main/resources/images/airflow_automation.png)

### 6. Visualization
  * Tableau is connected to Snowflake (initially live, now extracted).
  * The final dashboard is built to explore trends, KPIs, and insights.
  * Tool: Tableau Cloud for dashboard building and Tableau Public for publishing.

## 📑 Data Source Summary
  * Origin: Manually logged Google Sheet tracking beauty product purchases.
  * Tracked Fields: Product name (as written by the store), short name (actual product name), purchase date, product category, product type, product purpose, brand, store, quantity, unit price, and total price.
  * Added fields: Product name (brand + '-' + short name), price category (low, medium, and high - created from unit price).
  * ETL Flow: Google Sheets → Python (extraction, clean/transform) → Snowflake (raw, fact/dim tables) → Tableau (dashboard).
  * Current State: Due to platform limitations, Tableau Public is used for final publishing with an extracted version of the Snowflake data.

## 📊 Final Output: Dashboard
A Tableau interactive dashboard was created to explore and communicate the insights derived from the data. It includes:
  * Overview Dashboard: Annual trends, spending summaries, top products, stores & brands, and top 10 recent purchases.
  * Product Dashboard: Product analysis and detailed table of all purchased items with filters/sort.
  * Brand Dashboard: Brand analysis detailed table of all brands with filters/sort.
  * Dashboard Documentation: Reference material on navigating and using the dashboard.

> **🔗 View the Dashboard on [Tableau Public](https://public.tableau.com/views/BeautyProductsPurchaseDashboard/SummaryDashboard)**

![img](https://github.com/gloryodeyemi/Beauty_Purchase_Analysis/blob/main/resources/images/summary-dashboard.png)
![img](https://github.com/gloryodeyemi/Beauty_Purchase_Analysis/blob/main/resources/images/product-list.png)

## 📁 Project Structure
```
.
├── airflow/
│   ├── dags/
│   │   ├── beauty_pipeline.py       # Airflow DAG definition
│   │   └── etl/                     # Python scripts for each ETL stage
│   │       ├── extract.py           # Extracts data from Google Sheets
│   │       ├── transform.py         # Cleans and prepares data
│   │       └── load.py              # Loads data into Snowflake raw table
│   ├── data/
│   │   └── purchase_data.csv        # Backup of cleaned & transformed data
│   ├── docker-compose.yaml          # Docker setup for Airflow
│   ├── Dockerfile                   # Dockerfile for Airflow environment
│   └── requirements.txt             # Python dependencies
│
├── resources/
│   ├── dashboard-images/            # Images used for dashboard background styling
│   ├── icons/                       # Icons used in the dashboard
│   └── images/                      # Snapshots used in README
│
├── snowflake/
│   ├── data_model.sql               # SQL to create Snowflake schema, tables, roles, etc.
│   └── stored_proc.sql              # SQL stored procedure for transforming & inserting data
│
├── .gitignore
└── README.md
```

## 👩🏽‍💻 Creator
Glory Odeyemi - Data Engineer & Analyst
- For questions, feedback, opportunities, or collaborations, connect with me via [LinkedIn](https://www.linkedin.com/in/glory-odeyemi/).
- For more exciting projects or inspiration, check out my [GitHub repositories](https://github.com/gloryodeyemi).
