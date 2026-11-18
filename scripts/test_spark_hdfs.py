
from pyspark.sql import SparkSession

# Создание SparkSession
spark = (
    SparkSession.builder
    .appName("Spark HDFS Test")
    .getOrCreate()
)

# Проверка адреса HDFS из конфигурации Hadoop
hdfs_address = spark.sparkContext._jsc.hadoopConfiguration().get(
    "fs.defaultFS"
)
print("HDFS address:", hdfs_address)

# Создание небольшого тестового набора данных
data = [
    (1, "test"),
    (2, "success")
]

df = spark.createDataFrame(data, ["id", "status"])

# Сохранение данных в HDFS в формате Parquet
hdfs_path = "hdfs://namenode:8020/data/curated/_spark_test"

df.write.mode("overwrite").parquet(hdfs_path)

# Чтение данных из HDFS
result = spark.read.parquet(hdfs_path)

print("Number of rows:", result.count())
result.show()

spark.stop()
