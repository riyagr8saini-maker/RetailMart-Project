import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from delta import configure_spark_with_delta_pip


os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

# Defining folder paths
BRONZE_DIR = os.path.join("..", "data", "bronze")
SILVER_DIR = os.path.join("..", "data", "silver")
os.makedirs(SILVER_DIR, exist_ok=True)


def process_silver_layer():
    print("====== Starting Silver Layer Transformation ======\n")

    print(" Processing Orders with Pandas...")
    orders_path = os.path.join(BRONZE_DIR, "raw_orders_dataset.csv")
    df_orders = pd.read_csv(orders_path)

    col_order = 'order_purchase_timestamp'
    col_delivery = 'order_delivered_customer_date'

    if col_order in df_orders.columns and col_delivery in df_orders.columns:
       
        df_orders[col_order] = pd.to_datetime(df_orders[col_order], errors='coerce')
        df_orders[col_delivery] = pd.to_datetime(df_orders[col_delivery], errors='coerce')
        
        df_orders['delivery_days'] = (df_orders[col_delivery] - df_orders[col_order]).dt.days
        
        # Fill missing delivery days 
        df_orders['delivery_days'] = df_orders['delivery_days'].fillna(0).astype(int)
        print("  Calculated 'delivery_days' successfully using actual dataset columns.")
    else:
        print("   Warning: Date columns not found. Check exact column headers in raw_orders_dataset.csv")

    # Save cleaned orders back to Silver
    clean_orders_path = os.path.join(SILVER_DIR, "cleaned_orders.csv")
    df_orders.to_csv(clean_orders_path, index=False)

    
    # PYSPARK & DELTA LAKE: Product Catalogue SCD2
    print("\n Booting up PySpark for Delta Lake processing...")
    
    # Configure PySpark to use Delta Lake
    builder = SparkSession.builder.appName("RetailMart_Silver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    print(" Processing Products with SCD2 tracking...")
    products_path = os.path.join(BRONZE_DIR, "raw_products_dataset.csv")
    df_products = spark.read.csv(products_path, header=True, inferSchema=True)

    # Implement SCD2 columns: Tracking history if product attributes change
    df_products_scd2 = df_products \
        .withColumn("is_active", lit(True)) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit(None).cast("date"))

    # Save as a Delta Table
    delta_products_path = os.path.join(SILVER_DIR, "products_delta")
    df_products_scd2.write.format("delta").mode("overwrite").save(delta_products_path)
    print("  Products saved as Delta Table.")


    # PYSPARK: Move remaining files to Silver

    print("\n Migrating remaining datasets to Silver...")
    remaining_files = ["raw_customers_dataset.csv", "raw_items_dataset.csv", "raw_payments_dataset.csv"]
    
    for file in remaining_files:
        path = os.path.join(BRONZE_DIR, file)
        df_temp = spark.read.csv(path, header=True, inferSchema=True)
        
        # Save as clean CSV in silver
        silver_file_path = os.path.join(SILVER_DIR, file.replace("raw_", "cleaned_"))
        df_temp.toPandas().to_csv(silver_file_path, index=False)
        print(f"   🔹 Cleaned and saved {file.replace('raw_', 'cleaned_')}")

    print("\n====== Silver Layer Transformation Complete ======")

if __name__ == "__main__":
    process_silver_layer()