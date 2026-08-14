import os

python_path = r"C:\Users\BhargaviKummara\AppData\Local\Programs\Python\Python311\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('WordCount').getOrCreate()

documents = [(1, "Where there is a will there is a way"),
(2, "A stitch in time saves nine"),
(3, "A friend in need is a friend indeed"),
(4, "All that glitters is not gold")
]

documents_df = spark.createDataFrame(documents, "doc_id INT NOT NULL, text STRING")
# documents_df.show()

df_exploded = documents_df.withColumn("word", F.explode((F.split(F.lower(F.col("text")), " "))))
# df_exploded.show()
result = df_exploded.groupBy("word").agg(F.count("*").alias("count")).orderBy(F.col("count").desc(), F.col("word"))
result.show()

print("\n All jobs done. Spark UI is live at http://localhost:4040")
print(" Browse the UI, then press Enter here to shut Spark down...")
input() # script pauses here - Spark stays running, UI stays open
spark.stop() #cleanly shuts down after you press enter