# 🛠️  End-to-End Data Pipeline for Beauty Products Purchase Analysis
An end-to-end data engineering and analysis project that builds an automated data pipeline from Google Sheets to Snowflake, orchestrated with Apache Airflow and visualized in Tableau. This project captures real-world personal beauty product purchases, transforming raw entries into structured insights.

## 📌 Project Overview
This project analyzes real-world beauty product purchase data through a robust data pipeline and visualization framework. It was designed to:
* Track personal purchase behaviors and product preferences.
* Uncover trends in spending, product types, and brand loyalty.
* Serve as a template for others to understand purchase patterns using real-life data and modern analytics tools.

## 🧰 Tech Stack
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
    
  ![img](https://github.com/gloryodeyemi/Beauty_Purchase_Analysis/blob/main/resources/images/Beauty-data-model.png)

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
  * Tracked Fields: Date_Bought, Product_Name (as written by the store), Short_Name (actual product name), Product_Category, Product_Type, Product_Purpose, Brand, Store, Quantity, Unit_Price, and Total_Price.
  * Modified fields: Date_Bought, Product_Name (Brand + '-' + Short_Name), Product_Category, Product_Type, Product_Purpose, Brand, Store, Quantity, Unit_Price, Total_Price, and Price_Category (low, medium, and high - created from unit price).
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
![img](https://github.com/gloryodeyemi/Beauty_Purchase_Analysis/blob/main/resources/images/RPList.png)

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

## 📝 Recreating/Reusing This Project
To successfully recreate/reuse this project for your own data pipeline and dashboard, follow the steps below:

#### 1. Clone the Repository
```
git clone https://github.com/gloryodeyemi/Beauty_Purchase_Analysis.git
cd beauty-purchase-pipeline
```

#### 2. Create Required Accounts
You'll need the following accounts:
  * Snowflake (free trial)
  * Tableau Cloud (Free trial or full version)
  * Google Cloud Platform (GCP) for Google Sheets API access

#### 3. Snowflake Setup
* Log in to your Snowflake account.
* Execute the SQL scripts in the snowflake/ folder:
  - data_model.sql: Sets up warehouse, roles, database, schemas, and tables.
  - stored_proc.sql: Creates stored procedures for data transformation and insertion.

#### 4. Google Sheets API Access
If you’re pulling data from Google Sheets:
  * Go to Google Cloud Console.
  * Create a new project or select an existing one.
  * Enable the Google Sheets API and Google Drive API.
  * Create a service account and download the JSON key.
  * Share your Google Sheet with the service account email.
  * Add the JSON key file path to your project environment variable.

#### 5. Set Up Airflow Environment (with Docker)
* Navigate to the airflow/ directory.
* Start Airflow using Docker Compose:
```
docker compose up --build -d
```
* Access the Airflow web UI at http://localhost:8080.
* Navigate to Admin -> Connections and create a new Snowflake and Google connections using your account information.
* Trigger the beauty_purchase_pipeline DAG to extract, transform, and load the data into Snowflake.

#### 6. Verify Data in Snowflake
* Use Snowflake's UI to query your fact and related dimension tables.
* Confirm that your data is correctly inserted and transformed.

#### 7. Visualize with Tableau Cloud
* Sign in to Tableau Cloud.
* Connect Tableau to your Snowflake database.
* Use the Snowflake credentials and warehouse info configured in data_model.sql.
* Recreate or import the dashboard using images & icons in resources/.

## 💡 Future Imporovements
1. **Deploy the Pipeline to the Cloud:** Host the existing Airflow pipeline on a cloud platform (e.g., AWS EC2) and connect it to a managed Snowflake instance for seamless scheduling and scalability.
2. **Add Predictive Analytics:** Train and integrate a simple regression or time-series model within Snowflake (using Snowpark) to forecast future product demand.
3. **Implement Notifications:** Set up email notifications for pipeline failures and completions.

## 👩🏽‍💻 Creator
Glory Odeyemi - Data Engineer & Analyst
- For questions, feedback, opportunities, or collaborations, connect with me via [LinkedIn](https://www.linkedin.com/in/glory-odeyemi/).
- For more exciting projects or inspiration, check out my [GitHub repositories](https://github.com/gloryodeyemi).
