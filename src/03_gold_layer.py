import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

# Define paths
SILVER_DIR = os.path.join("..", "data", "silver")
GOLD_DIR = os.path.join("..", "data", "gold")
os.makedirs(GOLD_DIR, exist_ok=True)

def process_gold_layer():
    print("====== Starting Gold Layer (Business Analytics) ======\n")
    
    #  Boot up Spark with Delta Lake 
    builder = SparkSession.builder.appName("RetailMart_Gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #  Load Silver Data and register as temporary SQL views
    print(" Loading Silver data into SQL Engine...")
    
    # CORRECTED FILE NAMES
    spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
        .load(os.path.join(SILVER_DIR, "cleaned_orders.csv")).createOrReplaceTempView("orders")
        
    spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
        .load(os.path.join(SILVER_DIR, "cleaned_customers_dataset.csv")).createOrReplaceTempView("customers")
        
    spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
        .load(os.path.join(SILVER_DIR, "cleaned_items_dataset.csv")).createOrReplaceTempView("items")
        
    spark.read.format("csv").option("header", "true").option("inferSchema", "true") \
        .load(os.path.join(SILVER_DIR, "cleaned_payments_dataset.csv")).createOrReplaceTempView("payments")
    
    # Load the Delta Table
    spark.read.format("delta").load(os.path.join(SILVER_DIR, "products_delta")).createOrReplaceTempView("products")

    print("   🔹 All Silver tables successfully loaded into memory views.")

    #  Execute Business Queries 
    print("\n Generating: Customer 360 (JOINs)...")
    customer_360 = spark.sql("""
        SELECT 
            c.customer_id, c.customer_unique_id, c.customer_city, c.customer_state,
            COUNT(o.order_id) as total_orders
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.customer_unique_id, c.customer_city, c.customer_state
    """)
    customer_360.write.format("csv").option("header", "true").mode("overwrite").save(os.path.join(GOLD_DIR, "customer_360"))

    print(" Generating: Monthly Revenue (GROUP BY)...")
    monthly_revenue = spark.sql("""
        SELECT 
            DATE_FORMAT(CAST(order_purchase_timestamp AS TIMESTAMP), 'yyyy-MM') AS month,
            SUM(p.payment_value) as total_revenue
        FROM orders o
        JOIN payments p ON o.order_id = p.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY month
        ORDER BY month
    """)
    monthly_revenue.write.format("csv").option("header", "true").mode("overwrite").save(os.path.join(GOLD_DIR, "monthly_revenue"))

    print(" Generating: Customer Segments (CASE)...")
    customer_segments = spark.sql("""
        SELECT 
            c.customer_id,
            SUM(p.payment_value) as total_spend,
            CASE 
                WHEN SUM(p.payment_value) > 1000 THEN 'VIP'
                WHEN SUM(p.payment_value) BETWEEN 100 AND 1000 THEN 'Regular'
                ELSE 'Low Value'
            END AS segment
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        JOIN payments p ON o.order_id = p.order_id
        GROUP BY c.customer_id
    """)
    customer_segments.write.format("csv").option("header", "true").mode("overwrite").save(os.path.join(GOLD_DIR, "customer_segments"))

    print(" Generating: Trending Products by Category (Window Functions)...")
    trending_products = spark.sql("""
        WITH ProductSales AS (
            SELECT 
                pr.product_category_name as category,
                pr.product_id,
                COUNT(i.order_id) as total_sold
            FROM items i
            JOIN products pr ON i.product_id = pr.product_id
            WHERE pr.is_active = true
            GROUP BY category, pr.product_id
        )
        SELECT * FROM (
            SELECT 
                category,
                product_id,
                total_sold,
                RANK() OVER(PARTITION BY category ORDER BY total_sold DESC) as rank
            FROM ProductSales
            WHERE category IS NOT NULL
        ) WHERE rank <= 5
    """)
    trending_products.write.format("csv").option("header", "true").mode("overwrite").save(os.path.join(GOLD_DIR, "trending_products"))

    print(" Generating: Above Average Spenders (Subqueries)...")
    above_avg_spenders = spark.sql("""
        SELECT customer_id, total_spend
        FROM (
            SELECT c.customer_id, SUM(p.payment_value) as total_spend
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            JOIN payments p ON o.order_id = p.order_id
            GROUP BY c.customer_id
        )
        WHERE total_spend > (
            SELECT AVG(payment_value) FROM payments
        )
    """)
    above_avg_spenders.write.format("csv").option("header", "true").mode("overwrite").save(os.path.join(GOLD_DIR, "above_average_spenders"))

    print(" Generating: Funnel Analysis (CTEs)...")
    funnel_analysis = spark.sql("""
        WITH TotalCustomers AS (
            SELECT COUNT(DISTINCT customer_id) as total_customers FROM customers
        ),
        CustomersWithOrders AS (
            SELECT COUNT(DISTINCT customer_id) as ordering_customers FROM orders
        ),
        CustomersWithPayments AS (
            SELECT COUNT(DISTINCT o.customer_id) as paying_customers 
            FROM orders o
            JOIN payments p ON o.order_id = p.order_id
        )
        SELECT 
            t.total_customers,
            o.ordering_customers,
            p.paying_customers
        FROM TotalCustomers t, CustomersWithOrders o, CustomersWithPayments p
    """)
    funnel_analysis.write.format("csv").option("header", "true").mode("overwrite").save(os.path.join(GOLD_DIR, "funnel_analysis"))

    print("\n ====== Gold Layer Complete ======")

if __name__ == "__main__":
    process_gold_layer()