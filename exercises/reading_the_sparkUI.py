import os

python_path = r"C:\Users\BhargaviK\AppData\Local\Programs\Python\Python311\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('exercise1_sales').config("spark.ui.port", "4040").getOrCreate() # explicitly declaring port (4040 is default)
columns = ["product_id","date","sales"]
data = [("101","2023-12-01",100),("101","2023-12-02",150),("102","2023-12-01",200),("102","2023-12-02",250)]
df = spark.createDataFrame(data, columns)
print("****Sales data by products****")
df.show()

print("*****Count of products sold****")
df.groupBy("product_id").count().show()

print("*****Sum of sales per product*****")
df.groupBy("product_id").sum("sales").show()

print("\n All jobs done. Spark UI is live at http://localhost:4040")
print(" Browse the UI, then press Enter here to shut Spark down...")

input() # script pauses here - Spark stays running, UI stays open
spark.stop() #cleanly shuts down after you press enter
