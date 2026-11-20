
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Создаем SparkSession
spark = SparkSession.builder \
    .appName("US Accidents - Data Cleaning") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Читаем исходный CSV из HDFS
df = spark.read \
    .option("header", True) \
    .csv("hdfs://namenode:8020/data/raw/US_Accidents_March23.csv")

df.select(
    "ID",
    "Severity",
    "Start_Time",
    "City",
    "State",
    "Temperature(F)",
    "Weather_Condition"
).show(10, truncate=False)

# Столбцы, которые проверим
columns = [
    "ID",
    "Severity",
    "Start_Time",
    "Start_Lat",
    "Start_Lng",
    "Temperature(F)",
    "Visibility(mi)",
    "Weather_Condition"
]

# Считаем пропущенные значения
missing = df.agg(
    F.count("*").alias("total_rows"),
    *[
        F.sum(
            F.when(
                F.col(c).isNull() | (F.trim(F.col(c)) == ""),
                1
            ).otherwise(0)
        ).alias(c)
        for c in columns
    ]
)

missing.show(vertical=True, truncate=False)

spark.stop()
