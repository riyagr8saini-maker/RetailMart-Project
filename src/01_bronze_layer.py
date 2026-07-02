import os
import pandas as pd

# Define relative paths 
RAW_DIR = os.path.join("..", "data", "raw")
BRONZE_DIR = os.path.join("..", "data", "bronze")

# Ensure the bronze directory exists
os.makedirs(BRONZE_DIR, exist_ok=True)

# List of 5 datasets
datasets = [
    "raw_customers_dataset.csv",
    "raw_items_dataset.csv",
    "raw_orders_dataset.csv",
    "raw_payments_dataset.csv",
    "raw_products_dataset.csv"
]

def ingest_to_bronze():
    print("====== Starting Bronze Layer Ingestion ======\n")
    
    for file_name in datasets:
        raw_file_path = os.path.join(RAW_DIR, file_name)
        bronze_file_path = os.path.join(BRONZE_DIR, file_name)
        
        # Checking if raw file exists before reading
        if not os.path.exists(raw_file_path):
            print(f" Error: File not found at {raw_file_path}")
            continue
            
        print(f"Reading {file_name} from Raw storage...")
        # Load CSV using Pandas 
        df = pd.read_csv(raw_file_path)
        
        row_count = len(df)
        print(f" {file_name} loaded successfully. Total Rows: {row_count}")
        
        if "orders" in file_name:
            print(f" Verified: Found {row_count} total orders to process.")
            
        df.to_csv(bronze_file_path, index=False)
        print(f" Successfully saved copy to: {bronze_file_path}\n")
        
    print("====== Bronze Layer Ingestion Complete ======")

if __name__ == "__main__":
    ingest_to_bronze()