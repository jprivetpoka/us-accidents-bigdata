
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Создаём SparkSession
spark = (
    SparkSession.builder
    .appName("US Accidents - Weather Analytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. Читаем очищенный Parquet из HDFS
df = spark.read.parquet(
    "hdfs://namenode:8020/data/curated/accidents/"
)

# 3. Находим 8 самых распространённых погодных условий
valid_weather = df.filter(
    F.col("Weather_Condition").isNotNull()
    & (F.trim(F.col("Weather_Condition")) != "")
)

top_weather = [
    row["Weather_Condition"]
    for row in (
        valid_weather
        .groupBy("Weather_Condition")
        .count()
        .orderBy(
            F.desc("count"),
            F.asc("Weather_Condition")
        )
        .limit(8)
        .collect()
    )
]

print("Основные погодные условия:", top_weather)

# 4. Объединяем редкие и отсутствующие значения
df_weather = df.withColumn(
    "weather_group",
    F.when(
        F.col("Weather_Condition").isNull()
        | (F.trim(F.col("Weather_Condition")) == ""),
        "Unknown"
    )
    .when(
        F.col("Weather_Condition").isin(top_weather),
        F.col("Weather_Condition")
    )
    .otherwise("Other")
)

# 5. Агрегируем данные по годам и погодным условиям
weather_stats = (
    df_weather
    .groupBy("year", "weather_group")
    .agg(
        F.count("*").alias("accident_count"),
        F.sum(
            F.when(
                F.col("Severity").between(3, 4), 1
            ).otherwise(0)
        ).alias("high_severity_count")
    )
    .withColumn(
        "high_severity_pct",
        F.round(
            F.col("high_severity_count")
            / F.col("accident_count") * 100,
            2
        )
    )
)

# 6. Проверяем результаты
weather_stats.orderBy(
    F.desc("accident_count")
).show(15, truncate=False)

total = weather_stats.agg(
    F.sum("accident_count").alias("total")
).first()["total"]

print("Общее количество ДТП:", total)

if total != 7728394:
    raise ValueError("Количество ДТП не совпадает")

# 7. Настраиваем подключение к PostgreSQL
jdbc_options = {
    "url": "jdbc:postgresql://postgres:5432/analytics",
    "user": "postgres",
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
    "dbtable": "public.accidents_by_weather_year"
}

# 8. Записываем агрегаты в PostgreSQL
(
    weather_stats.write
    .format("jdbc")
    .options(**jdbc_options)
    .mode("errorifexists")
    .save()
)

print("Таблица accidents_by_weather_year создана")

spark.stop()
