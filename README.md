
# US Accidents — Big Data Analytics

Проект анализа дорожно-транспортных происшествий в США на основе датасета US Accidents (March 2023).

Для хранения и обработки больших данных используется локальная инфраструктура на базе Docker Compose.

## 1. Архитектура проекта

Проект включает семь Docker-контейнеров:

| Сервис | Назначение |
|---|---|
| Hadoop NameNode | Управление файловой системой HDFS |
| Hadoop DataNode 1 | Хранение блоков данных |
| Hadoop DataNode 2 | Хранение блоков данных |
| Spark Master | Управление выполнением Spark-задач |
| Spark Worker | Выполнение вычислений |
| PostgreSQL | Хранение результатов анализа |
| Apache Superset | Визуализация данных и создание дашбордов |

Основной поток обработки данных:

**CSV → HDFS → Apache Spark → PostgreSQL → Apache Superset**

HDFS хранит исходный датасет. Spark выполняет его обработку и анализ. Результаты записываются в PostgreSQL и используются для визуализации в Superset.

## 2. Требования

Для запуска необходимы:

- Docker Desktop с поддержкой Docker Compose
- Git

Все компоненты запускаются локально. Отдельный облачный сервер не требуется.

## 3. Первый запуск

### 3.1. Скачивание проекта

Клонируйте репозиторий и перейдите в папку проекта:

```bash
git clone https://github.com/jprivetpoka/us-accidents-bigdata.git
cd us-accidents-bigdata
```

### 3.2. Настройка переменных окружения

Создайте файл `.env` на основе шаблона:

```bash
cp .env.example .env
```

Откройте `.env` и заполните переменные:

- `POSTGRES_PASSWORD` — задайте пароль PostgreSQL.
- `SUPERSET_SECRET_KEY` — сгенерируйте секретный ключ Superset.
- `SUPERSET_ADMIN_PASSWORD` — задайте пароль администратора Superset.

Для генерации секретного ключа можно использовать:

```bash
openssl rand -hex 32
```

Скопируйте полученное значение в `SUPERSET_SECRET_KEY`.

Не добавляйте настоящий `.env` с паролями в Git.

### 3.3. Запуск инфраструктуры

В корневой папке проекта выполните:

```bash
docker compose up -d
```

Docker Compose создаст и запустит семь контейнеров.

Проверьте состояние сервисов:

```bash
docker compose ps
```

При первом запуске PostgreSQL инициализирует базы данных `analytics` и `superset_meta` в новом Docker volume.

Superset использует `superset_meta` для хранения своих настроек и пользователей.

## 4. Веб-интерфейсы

После запуска доступны:

| Компонент | Адрес |
|---|---|
| Hadoop NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8082 |
| Apache Superset | http://localhost:8088 |

Для входа в Superset используется пользователь `admin` и пароль из `SUPERSET_ADMIN_PASSWORD`.

Локальное подключение к PostgreSQL:

- Host внутри Docker: `postgres`
- Port внутри Docker: `5432`
- Port на компьютере: `5435`
- Database: `analytics`
- User: `postgres`

## 5. Управление контейнерами

Остановить контейнеры без удаления данных:

```bash
docker compose down
```

Повторно запустить:

```bash
docker compose up -d
```

Посмотреть состояние:

```bash
docker compose ps
```

Посмотреть логи конкретного сервиса:

```bash
docker compose logs --tail=100 spark-master
```

Данные HDFS и PostgreSQL сохраняются в Docker volumes при обычной остановке.

**Внимание:** команда `docker compose down -v` удаляет Docker volumes и может привести к потере данных.

## 6. Хранение данных в HDFS

Для хранения исходных и обработанных данных используется распределённая файловая система Hadoop HDFS.

В проекте предусмотрена следующая структура:

```text
/data/
├── raw/
│   └── US_Accidents_March23.csv
└── curated/
    └── accidents/  (будущие обработанные данные в Parquet)
```
На схеме представлены каталоги файловой системы HDFS, а не локальной папки проекта data/. Физически данные HDFS хранятся в Docker volumes на компьютере.
- **`/data/raw/`** — исходные данные без изменений.
- **`/data/curated/`** — очищенные и подготовленные данные после обработки в Apache Spark.

Исходный датасет `US_Accidents_March23.csv` содержит 7 728 394 записи и 46 столбцов.

### 6.1. Подготовка каталогов HDFS

После запуска Docker Compose создайте каталоги:

```bash
docker compose exec namenode \
  hdfs dfs -mkdir -p /data/raw /data/curated
```

Команда создаёт оба каталога, если они ещё не существуют. При повторном выполнении существующие каталоги сохраняются.

Проверьте структуру:

```bash
docker compose exec namenode hdfs dfs -ls /data
```

Ожидается наличие каталогов `raw` и `curated`.

### 6.2. Загрузка исходного CSV

Ссылка на датасет: https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents/data
Поместите файл `US_Accidents_March23.csv` в папку `data/` в корне проекта:

```text
us-accidents-bigdata/
└── data/
    └── US_Accidents_March23.csv
```

Затем загрузите файл в HDFS:

```bash
docker compose exec -T namenode \
  hdfs dfs -put - /data/raw/US_Accidents_March23.csv \
  < data/US_Accidents_March23.csv
```

Команда передаёт файл из локальной папки проекта в HDFS.

Если CSV уже находится в `/data/raw/`, повторная загрузка не требуется.

### 6.3. Проверка исходных данных

Посмотрите содержимое каталога:

```bash
docker compose exec namenode \
  hdfs dfs -ls -h /data/raw
```

Проверьте целостность CSV:

```bash
docker compose exec namenode \
  hdfs fsck /data/raw/US_Accidents_March23.csv
```

Ожидаемый статус: `HEALTHY`.

### 6.4. Хранение обработанных данных

После очистки и преобразования исходного CSV с помощью Apache Spark результаты будут сохраняться в каталоге `/data/curated/`.

Планируемый формат хранения — **Apache Parquet**, который удобен для аналитической обработки больших данных.

Например, результаты очистки могут размещаться по пути:

```text
hdfs://namenode:8020/data/curated/accidents/
```

Каталог `accidents/` будет создан при первой записи обработанных данных Spark. На текущем этапе основной очищенный датасет ещё не сформирован.

## 7. Работа с Apache Spark

PySpark-скрипты находятся в папке `scripts/`, которая подключается к контейнеру Spark Master.

### 7.1. Проверка чтения CSV

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  /opt/spark/work/inspect_raw.py
```

Скрипт проверяет доступность исходного датасета через Spark.

Ожидаемые характеристики:
- Записи: 7 728 394
- Столбцы: 46

### 7.2. Проверка PostgreSQL

Для проверки подключения Spark к PostgreSQL:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  /opt/spark/work/test_spark_postgres.py
```

Ожидается значение `connection_ok = 1`.

JDBC-драйвер PostgreSQL подключается автоматически через `config/spark/spark-defaults.conf`.

## 8. Резервное копирование PostgreSQL

Для передачи результатов анализа между участниками используется резервная копия базы `analytics`.

База `superset_meta` хранит настройки Superset и не включается в эту резервную копию.

### 8.1. Создание резервной копии

Выполните:

```bash
docker compose exec -T postgres \
  pg_dump -U postgres -d analytics \
  -Fc --no-owner --no-acl \
  > backups/analytics.dump
```

Файл `backups/analytics.dump` содержит структуру и данные таблиц базы `analytics`.

### 8.2. Проверка резервной копии

```bash
docker compose exec -T postgres \
  pg_restore --list < backups/analytics.dump
```

Команда выводит содержимое архива без восстановления базы.

### 8.3. Восстановление базы

Получите файл `analytics.dump` от участника, создавшего резервную копию, и поместите его в папку `backups/`.

Восстановление выполняется в новую, пустую базу `analytics`, созданную при первом запуске PostgreSQL:

```bash
docker compose exec -T postgres \
  pg_restore -U postgres \
  -d analytics \
  --no-owner --no-acl --exit-on-error \
  < backups/analytics.dump
```

Важно: эта команда предназначена для пустой базы. Если в `analytics` уже имеются таблицы, восстановление может привести к конфликтам. Перед заменой существующих данных необходимо отдельно создать резервную копию.

### 8.4. Проверка восстановленных таблиц

```bash
docker compose exec postgres \
  psql -U postgres -d analytics \
  -c "\dt"
```

Команда отображает таблицы базы `analytics`.

## 9. Работа с Apache Superset

Откройте http://localhost:8088 и войдите под пользователем `admin`.

Если подключение `US Accidents Analytics` ещё не создано, добавьте базу PostgreSQL через интерфейс управления подключениями Superset.

Параметры:

- Host: `postgres`
- Port: `5432`
- Database: `analytics`
- Username: `postgres`
- Password: значение `POSTGRES_PASSWORD` из `.env`

В SQL Lab выберите подключение к `analytics` и выполните проверочный запрос:

```sql
SELECT 1 AS connection_ok;
```

Ожидаемый результат: `1`.

После загрузки аналитических таблиц их можно использовать для создания датасетов, графиков и дашбордов.

## 10. Совместная работа

В Git хранятся код проекта, Docker Compose, конфигурационные файлы и инструкции.

Исходный CSV и данные Docker volumes автоматически не передаются через Git.

Для обмена обработанными данными используется файл `analytics.dump`. Способ его передачи определяется командой с учётом размера файла.

Не добавляйте в Git настоящий `.env`, пароли и другие секретные данные.
