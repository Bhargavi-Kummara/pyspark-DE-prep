import os

python_path = r"C:\Users\BhargaviK\AppData\Local\Programs\Python\Python311\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Practice_joins").getOrCreate()

# customers table
cust_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True)
])

cust_data = [
    (10, "Alice", "New York"),
    (20, "Bob", "London"),
    (30, "Charlie", "Paris"),
    (40, "David", "Tokyo"),
    (15, "Bhargavi", "India")
]

customers_df = spark.createDataFrame(cust_data, schema=cust_schema)

# orders table
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", IntegerType(), True)
])

orders_data = [
    (101, 10, 250),
    (102, 20, 150),
    (103, 10, 300),
    (104, 30, 450),
    (106, 20, 90),
    (107, 10, 500),
    (108, 999, 75),    
]

orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

print("Customers dataframe")
customers_df.show()
print("Orders dataframe")
orders_df.show()

inner_df = orders_df.join(customers_df, orders_df["customer_id"]==customers_df["customer_id"], how="inner")
'''
#SQL Equivalent:
SELECT * FROM orders
INNER JOIN customers
ON orders.customer_id = customers.customer_id
'''
print("Inner JOIN")
inner_df.show()

left_df = orders_df.join(customers_df, "customer_id", "left")
'''
#SQL Equivalent:
SELECT * FROM orders
LEFT JOIN customers
USING (customer_id)
'''
print("Left JOIN")
left_df.show()

# Right join is just a Left join with tables swapped
right_df = orders_df.join(customers_df, "customer_id", "right")
print("Right JOIN")
right_df.show()

# FULL OUTER JOIN - Union of left and right join
full_df = orders_df.join(customers_df, "customer_id", "full").orderBy("customer_id")
print("Full outer JOIN")
full_df.show()
print("Comparing with union of left and right df combined")
left_df.union(right_df).distinct().orderBy("customer_id").show()

# LEFT Semi JOIN - Rows from left that have a match in Right (but no RIGHT cols)
'''
#SQL equivalent:
SELECT * FROM orders 
WHERE customer_id IN (SELECT customer_id FROM customers)
'''
semi_df = orders_df.join(customers_df, "customer_id", "leftsemi")
print("Left Semi JOIN")
semi_df.show()

# LEFT Anti JOIN - Opposite of left semi, rows from Left that have NO match in RIGHT
'''
#SQL equivalent:
SELECT * FROM orders 
WHERE customer_id NOT in (SELECT customer_id FROM customers)
'''
anti_df = orders_df.join(customers_df, "customer_id", "leftanti")
print("Left ANTI join")
anti_df.show()

'''
When both DataFrames are large, Spark must shuffle — redistribute rows 
across the network so matching keys land on the same executor.

But Shuffles are expensive - network I/O, disk spills, and slow

BROADCAST join - send the small table to everyone
When one table is small, Spark can broadcast a copy to every executor.
No shuffle needed
'''
broadcast_df = orders_df.join(F.broadcast(customers_df), "customer_id", "inner")
print("Broadcast JOIN")
broadcast_df.show()

spark.stop()
