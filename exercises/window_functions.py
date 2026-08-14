import os

python_path = r"C:\Users\BhargaviKummara\AppData\Local\Programs\Python\Python311\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PySparkPractice2").getOrCreate()

data = [
    (1, 101, 500.0, "Completed", "2024-01-01"),
    (2, 101, 1200.0, "Completed", "2024-01-02"),
    (3, 101, 1200.0, "Completed", "2024-01-03"),
    (4, 101, 300.0, "Cancelled", "2024-01-04"),
    (5, 102, 800.0, "Completed", "2024-01-01"),
    (6, 102, 400.0, "Completed", "2024-01-02"),
    (7, 102, 950.0, "Completed", "2024-01-03"),
    (8, 103, 150.0, "Completed", "2024-01-01"),
]

schema = StructType([
    StructField("txn_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("txn_date", StringType(), True)
])

transactions = spark.createDataFrame(data, schema)

# define window 
w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())
result_df = transactions.filter(F.col("status")=="Completed")\
            .withColumn("rnk", F.dense_rank().over(w)).filter(F.col("rnk")<3)\
            .select("customer_id", "txn_id", "amount", "rnk")\
            .orderBy(F.col("customer_id").asc(), F.col("rnk").asc())

result_df.show()

'''
Task:
From the transactions DataFrame, find the top 2 highest transactions for 
each customer (customer_id).Include only transactions with status = 'Completed'.
Order the final result by customer_id ascending, then by rank ascending.
For ties in transaction amount, assign the same rank.
'''

spark.stop()