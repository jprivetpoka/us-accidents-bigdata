
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Создаём SparkSession
spark = (
    SparkSession.builder
    .appName("US Accidents - State Analytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. Читаем очищенные данные из HDFS
df = spark.read.parquet(
    "hdfs://namenode:8020/data/curated/accidents/"
)

# 3. Рассчитываем количество ДТП по штатам и годам
state_stats = (
    df.groupBy("year", "State")
    .agg(
        F.count("*").alias("accident_count"),
        F.sum(
            F.when(F.col("Severity").between(3, 4), 1)
            .otherwise(0)
        ).alias("high_severity_count")
    )
    .withColumnRenamed("State", "state")
)

# 4. Проверяем результаты агрегации
state_stats.orderBy(
    F.desc("accident_count")
).show(15, truncate=False)

total = state_stats.agg(
    F.sum("accident_count").alias("total")
).first()["total"]

print("Общее количество ДТП:", total)

if total != 7728394:
    raise ValueError("Количество ДТП не совпадает с исходным датасетом")

# 5. Подключаемся к локальному PostgreSQL
jdbc_options = {
    "url": "jdbc:postgresql://postgres:5432/analytics",
    "user": "postgres",
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
    "dbtable": "public.accidents_by_state_year"
}

# 6. Сохраняем агрегированную таблицу
(
    state_stats.write
    .format("jdbc")
    .options(**jdbc_options)
    .mode("errorifexists")
    .save()
)

print("Таблица accidents_by_state_year создана в PostgreSQL")

spark.stop()
