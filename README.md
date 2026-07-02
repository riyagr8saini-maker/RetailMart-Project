# RetailMart Data Analytics Platform 🛒

## 📌 Problem Statement
RetailMart's data was highly fragmented across raw CSV files (orders, customers, products, payments), making it impossible for the business team to answer critical questions regarding trending products, customer churn, and monthly revenue. 

## 🚀 Solution Architecture
This project implements a centralized Data Analytics Platform using the **Medallion Data Architecture** (Bronze, Silver, Gold layers) to ingest, clean, and transform the data into business-ready insights.

## 📊 Dataset
*Note: GitHub's file size limits prevent hosting the raw data directly in this repository.*

The raw datasets required to run this pipeline (Orders, Customers, Products, Items, Payments) can be downloaded from this **[Google Drive Link](https://drive.google.com/drive/folders/1SvbM_NzJlg7HboI2kzm2KsNgDIu0Pde5?usp=drive_link)**. 

**To test this pipeline locally:**
1. Download the CSV files from the drive.
2. Place them inside the `data/raw/` folder in this project.
3. Run the Python scripts in the `src/` folder to automatically generate the Bronze, Silver, and Gold layers.

### 🛠️ Tech Stack & Implementation
* **Python & Pandas:** Raw data ingestion and initial data profiling (calculating `delivery_days`).
* **PySpark:** Scalable data transformations and migrating historical data.
* **Delta Lake:** Implemented on the Product Catalogue to support **SCD Type 2** tracking (`is_active`, `start_date`, `end_date`).
* **Spark SQL:** Heavy business aggregations including:
  * `JOIN` operations for a Customer 360 view.
  * `GROUP BY` for Monthly Revenue tracking.
  * `CASE` statements for Customer Segmentation (VIP, Regular, Low Value).
  * `Window Functions` (`RANK() OVER`) to identify trending products per category.

---

## 📂 Project Structure
```text
retailmart_project/
│
├── data/
│   ├── raw/         # Original CSV datasets
│   ├── bronze/      # Unmodified landing zone data
│   ├── silver/      # Cleaned data & Delta Tables (SCD2)
│   └── gold/        # Aggregated business-level tables
│
├── src/
│   ├── 01_bronze_layer.py    # Ingestion script
│   ├── 02_silver_layer.py    # Transformation & Delta Lake script
│   ├── 03_gold_layer.py      # Business logic & SQL aggregations
│   └── query_tester.py       # Interactive SQL CLI tool
