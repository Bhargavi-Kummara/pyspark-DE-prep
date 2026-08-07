import os

python_path = r"C:\Users\BhargaviK\AppData\Local\Programs\Python\Python311\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Broadcast_join").getOrCreate()

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


'''
When both DataFrames are large, Spark must shuffle — redistribute rows 
across the network so matching keys land on the same executor.

But Shuffles are expensive - network I/O, disk spills, and slow

BROADCAST join - send the small table to everyone
When one table is small, Spark can broadcast a copy to every executor.
No shuffle needed
'''

# --- Shuffle join (disable auto-broadcast to force it) ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # disable auto-broadcast

shuffle_df = orders_df.join(customers_df, "customer_id", "inner")
print("*** SHUFFLE JOIN (SortMergeJoin) ***")
shuffle_df.explain()

# --- Broadcast join (re-enable) ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)  # restore default (10MB)

broadcast_df = orders_df.join(F.broadcast(customers_df), "customer_id", "inner")
print("*** BROADCAST JOIN ***")
broadcast_df.explain()

'''
What to look for in explain() output
Keywords in plan
----------------
BroadcastHashJoin: *Efficient — small table was broadcast
BroadcastExchange: The table being sent to all executors
SortMergeJoin: Both sides are large — sort + merge approach
Exchange hashpartitioning: *Shuffle — expensive network redistribution
BuildLeft / BuildRight: Which side of the join was broadcast

Rule of thumb: If one table fits in memory (< 10 MB by default, configurable), always use broadcast(). 
In production, the threshold is spark.sql.autoBroadcastJoinThreshold.
'''

spark.stop()
