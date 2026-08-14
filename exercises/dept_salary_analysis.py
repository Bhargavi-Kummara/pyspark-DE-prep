import os

python_path = r"C:\Users\BhargaviKummara\AppData\Local\Programs\Python\Python311\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PySparkPractice").getOrCreate()

data = [
    (101, "Alice", "IT", "Active", 85000.0),
    (102, "Bob", "IT", "Active", 75000.0),
    (103, "Charlie", "IT", "Inactive", 90000.0),
    (104, "David", "HR", "Active", 50000.0),
    (105, "Eva", "HR", "Active", 55000.0),
    (106, "Frank", "Finance", "Active", 110000.0),
    (107, "Grace", "Finance", "Active", 95000.0),
    (108, "Hank", "Finance", "Inactive", 120000.0),
]

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("status", StringType(), True),
    StructField("salary", DoubleType(), True)
])

employees = spark.createDataFrame(data, schema)

'''
find the total salary expenditure (total_salary) and average salary (avg_salary, rounded to 2 decimal places) 
for each department.Include only employees whose status is 'Active'.
Filter the grouped results to only include departments where the average salary is greater than 65,000.
Order the output by avg_salary in descending order.

select sum(salary) as total_salary, avg(salary) as avg_salary
from employees
where status='Active'
group by dept
having avg_salary > 65000
order by avg_salary desc
'''

result_df = employees.filter(F.col("status")=='Active')\
    .groupBy("dept").agg(
            F.sum("salary").alias("total_salary"),
            F.round(F.avg("salary"), 2).alias("avg_salary")
    )\
    .filter(F.col("avg_salary")>65000)\
    .orderBy(F.col("avg_salary").desc())


# result_df = (
#         employees.filter(F.col("status")=="Active")
#                         .groupBy(F.col("dept"))
#                         .agg(
#                             F.sum("salary").alias("total_salary"),
#                             F.round(F.avg("salary"), 2).alias("avg_salary")
#                         )
#                         .filter(F.col("avg_salary") > 65000)
#                         .orderBy(F.col("avg_salary").desc())
# )

result_df.show()