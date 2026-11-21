
from pyspark.sql import SparkSession

# Создаём SparkSession
spark = (
    SparkSession.builder
    .appName("Check Curated Accidents")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Читаем очищенные данные из HDFS
path = "hdfs://namenode:8020/data/curated/accidents/"

df = spark.read.parquet(path)

# Проверяем размер датасета
print("Количество строк:", df.count())
print("Количество столбцов:", len(df.columns))

# Проверяем названия и типы столбцов
print("\nСтруктура данных:")
df.printSchema()

# Проверяем признаки для будущих графиков
required_columns = [
    "Severity",
    "State",
    "Weather_Condition",
    "Sunrise_Sunset",
    "hour",
    "is_weekend",
    "year"
]

missing = [c for c in required_columns if c not in df.columns]

print("\nОтсутствующие необходимые столбцы:", missing)

spark.stop()
