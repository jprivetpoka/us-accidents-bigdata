
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("US Accidents - Raw Data Inspection")
    .getOrCreate()
)

# Путь к исходному датасету в HDFS
path = "hdfs://namenode:8020/data/raw/US_Accidents_March23.csv"

# Читаем CSV без автоматического определения типов
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(path)
)

# Проверяем структуру данных
print("Number of columns:", len(df.columns))
print("Column names:", df.columns)

# Показываем первые пять записей
df.select(
    "ID",
    "Severity",
    "Start_Time",
    "City",
    "State"
).show(5, truncate=False)

# Подсчёт общего количества записей
print("Number of rows:", df.count())

spark.stop()

