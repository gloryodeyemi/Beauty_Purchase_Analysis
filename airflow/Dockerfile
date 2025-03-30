# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.10.5
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install gspread
RUN pip install gspread