import os
from pyspark.sql import SparkSession

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

GOLD_DIR = os.path.join("..", "data", "gold")

def run_query_tester():
    print("\n====== Booting up RetailMart Analytics Engine ======")
    
    # 1. Start Spark
    spark = SparkSession.builder.appName("RetailMart_Tester").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Load the Gold Layer tables
    print(" Loading Business Tables into memory...")
    try:
        spark.read.csv(os.path.join(GOLD_DIR, "customer_360"), header=True, inferSchema=True).createOrReplaceTempView("customer_360")
        spark.read.csv(os.path.join(GOLD_DIR, "monthly_revenue"), header=True, inferSchema=True).createOrReplaceTempView("monthly_revenue")
        spark.read.csv(os.path.join(GOLD_DIR, "customer_segments"), header=True, inferSchema=True).createOrReplaceTempView("customer_segments")
        spark.read.csv(os.path.join(GOLD_DIR, "trending_products"), header=True, inferSchema=True).createOrReplaceTempView("trending_products")
        spark.read.csv(os.path.join(GOLD_DIR, "above_average_spenders"), header=True, inferSchema=True).createOrReplaceTempView("above_average_spenders")
        spark.read.csv(os.path.join(GOLD_DIR, "funnel_analysis"), header=True, inferSchema=True).createOrReplaceTempView("funnel_analysis")
    except Exception as e:
        print(f" Error loading tables. Make sure 03_gold_layer.py ran successfully. Details: {e}")
        return

    print("\n System Ready!")
    print("-" * 50)
    print("Available Tables to Query:")
    print(" 1. customer_360 (Columns: customer_id, customer_city, total_orders...)")
    print(" 2. monthly_revenue (Columns: month, total_revenue)")
    print(" 3. customer_segments (Columns: customer_id, total_spend, segment)")
    print(" 4. trending_products (Columns: category, product_id, total_sold, rank)")
    print(" 5. above_average_spenders (Columns: customer_id, total_spend)")
    print(" 6. funnel_analysis (Columns: total_customers, ordering_customers, paying_customers)")
    print("-" * 50)
    print("Type your SQL query below. Type 'exit' to quit.\n")

   
    while True:
        query = input("retailmart-sql> ")
        
        if query.lower() in ['exit', 'quit']:
            print("Shutting down engine. Goodbye!")
            break
            
        if not query.strip():
            continue

        # Try to execute the query, catch errors if they misspell something
        try:
           
            spark.sql(query).show(truncate=False)
        except Exception as e:
            print(f"\n Invalid SQL Query. Please check your syntax.\nError details: {e}\n")

if __name__ == "__main__":
    run_query_tester()