
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Создаём SparkSession
spark = (
    SparkSession.builder
    .appName("US Accidents - Time Analytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. Читаем очищенный Parquet из HDFS
df = spark.read.parquet(
    "hdfs://namenode:8020/data/curated/accidents/"
)

# 3. Подготавливаем признак дня и ночи
df_time = df.withColumn(
    "day_night",
    F.when(
        F.col("Sunrise_Sunset").isin("Day", "Night"),
        F.col("Sunrise_Sunset")
    ).otherwise("Unknown")
)

# 4. Считаем ДТП по году, часу, типу дня и освещению
time_stats = (
    df_time
    .groupBy("year", "hour", "is_weekend", "day_night")
    .agg(
        F.count("*").alias("accident_count"),
        F.sum(
            F.when(
                F.col("Severity").between(3, 4), 1
            ).otherwise(0)
        ).alias("high_severity_count")
    )
)

# 5. Проверяем результат агрегации
time_stats.orderBy(
    F.desc("accident_count")
).show(15, truncate=False)

totals = time_stats.agg(
    F.sum("accident_count").alias("total"),
    F.sum("high_severity_count").alias("high_total")
).first()

print("Общее количество ДТП:", totals["total"])
print("ДТП с Severity 3-4:", totals["high_total"])

if totals["total"] != 7728394:
    raise ValueError("Общее количество ДТП не совпадает")

if totals["high_total"] != 1504047:
    raise ValueError("Количество ДТП с Severity 3-4 не совпадает")

# 6. Подключаемся к локальному PostgreSQL
jdbc_options = {
    "url": "jdbc:postgresql://postgres:5432/analytics",
    "user": "postgres",
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
    "dbtable": "public.accidents_by_time"
}

# 7. Записываем результат через JDBC
(
    time_stats.write
    .format("jdbc")
    .options(**jdbc_options)
    .mode("errorifexists")
    .save()
)

print("Таблица accidents_by_time создана в PostgreSQL")

spark.stop()
