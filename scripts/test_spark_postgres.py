
import os
from pyspark.sql import SparkSession

# Параметры локального PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/analytics"

# Создаём SparkSession
spark = (
    SparkSession.builder
    .appName("TestLocalPostgreSQL")
    .getOrCreate()
)

# Выполняем проверочный запрос через JDBC
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "(SELECT 1 AS connection_ok) AS test")
    .option("user", "postgres")
    .option("password", os.environ["POSTGRES_PASSWORD"])
    .option("driver", "org.postgresql.Driver")
    .load()
)

df.show()

spark.stop()
