# US Accidents — Big Data Analytics

Командный проект по анализу дорожно-транспортных происшествий в США на датасете **US Accidents (March 2023)**. Исходный CSV содержит **7 728 394 записи, 46 столбцов** и существенно превышает 0,5 ГБ. Обработка выполняется с использованием Hadoop HDFS и Apache Spark; результаты аналитики хранятся в PostgreSQL и отображаются в Apache Superset.

Гугл документ по этапам работы: https://docs.google.com/document/d/1A5e0463F-eojgZygB6DZNBpyIubzJymKjEN_AdXv7Uk/edit?usp=sharing

## 1. Архитектура и фактическое состояние проекта

Текущая конфигурация Docker Compose включает **7 контейнеров**:

| Сервис | Назначение |
|---|---|
| `namenode` | Метаданные файловой системы HDFS |
| `datanode-1`, `datanode-2` | Хранение блоков CSV и Parquet в HDFS (репликация — 2) |
| `spark-master` | Управление Spark-задачами |
| `spark-worker-1` | Выполнение Spark-задач |
| `postgres` | Базы `analytics` и `superset_meta` |
| `superset` | Интерактивный аналитический дашборд |

**Поток данных:**

```text
US Accidents CSV
    ↓
HDFS /data/raw/
    ↓ Spark / PySpark: очистка и дополнительные признаки
HDFS /data/curated/accidents/ (Parquet)
    ├──→ Spark MLlib и проверка гипотез
    └──→ Spark / JDBC → PostgreSQL analytics → Apache Superset
```

Проект организован в четыре этапа: **архитектура → обработка данных и ML → проверка гипотез → аналитическая БД и визуализация**.

## 2. Требования и структура файлов

Для работы необходимы Docker Desktop с Docker Compose, Git и достаточно свободного места для исходного CSV, HDFS и Parquet. Для работы с исследовательским ноутбуком требуется среда Jupyter/PySpark с доступом к Spark и HDFS.

```text
us-accidents-bigdata/
├── config/
│   ├── hadoop/
│   ├── spark/
│   └── superset/
├── data/                             # Локальные большие данные; не передаются через Git
├── docs/                             # Скрины и модель архитектуры
├── exports/
│   ├── analytics.sql                # Агрегаты PostgreSQL, без исходного CSV
│   └── superset_dashboard_safe.zip  # Экспорт Superset без пароля в SQLAlchemy URI
├── scripts/
│   ├── analytics/
│   │   ├── build_state_analytics.py
│   │   ├── build_weather_analytics.py
│   │   └── build_time_analytics.py
│   ├── exploration/
│   │   ├── data_exploration.ipynb
│   │   └── inspect_raw.py
│   ├── preprocessing/
│   │   └── clean_accidents.py
│   ├── tests/
│   │   ├── test_spark_hdfs.py
│   │   └── test_spark_postgres.py
│   └── validation/
│       └── check_curated.py
├── sql/
├── .env.example
├── docker-compose.yml
└── README.md
```

Большие файлы `data/`, настоящие пароли в `.env` и оригинальный экспорт Superset с паролем не следует добавлять в Git. Перед публикацией `exports/` необходимо дополнительно проверить SQL и ZIP на секреты.

## 3. Первый запуск инфраструктуры

### 3.1. Скачивание и переменные окружения

```bash
git clone https://github.com/jprivetpoka/us-accidents-bigdata.git
cd us-accidents-bigdata
cp .env.example .env
```

Заполните `.env`:

- `POSTGRES_PASSWORD` — пароль PostgreSQL;
- `SUPERSET_SECRET_KEY` — секретный ключ Superset;
- `SUPERSET_ADMIN_PASSWORD` — пароль администратора Superset.

Генерация ключа Superset:

```bash
openssl rand -hex 32
```

Не публикуйте `.env` и значения секретов.

### 3.2. Запуск контейнеров

```bash
docker compose up -d
docker compose ps
```

В текущей конфигурации PostgreSQL при первом запуске должен содержать базы `analytics` и `superset_meta`. Убедитесь в их наличии до восстановления аналитики.

### 3.3. Адреса сервисов

| Сервис | Адрес / параметры |
|---|---|
| HDFS NameNode | <http://localhost:9870> |
| Spark Master | <http://localhost:8082> |
| Apache Superset | <http://localhost:8088> |
| PostgreSQL из контейнеров | `postgres:5432` |
| PostgreSQL с компьютера | `localhost:5435` (согласно текущему README; проверить в Compose при изменении портов) |

В Superset входите как `admin` с паролем `SUPERSET_ADMIN_PASSWORD` из `.env`.

## 4. Исходные данные в HDFS

Скачать `US_Accidents_March23.csv` можно на [Kaggle](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents/data). Поместите файл в локальный каталог `data/` проекта.

Создайте каталоги в HDFS:

```bash
docker compose exec namenode hdfs dfs -mkdir -p /data/raw /data/curated
```

Загрузите CSV (если он ещё не загружен):

```bash
docker compose exec -T namenode \
  hdfs dfs -put - /data/raw/US_Accidents_March23.csv \
  < data/US_Accidents_March23.csv
```

Проверка:

```bash
docker compose exec namenode hdfs dfs -ls -h /data/raw
docker compose exec namenode hdfs fsck /data/raw/US_Accidents_March23.csv
```

Ожидается статус `HEALTHY`. Повторно загружать уже существующий файл не нужно.

## 5. Spark: исследование, очистка и Parquet

Папка `scripts/` целиком подключается в Spark-контейнер как `/opt/spark/work/`; вложенные папки также доступны.

### 5.1. Проверка чтения исходного CSV

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  /opt/spark/work/exploration/inspect_raw.py
```

Ожидаются **7 728 394 строк и 46 исходных столбцов**.

### 5.2. Получение готового Parquet или обработка с нуля

- Исследовательский ноутбук: `scripts/exploration/data_exploration.ipynb`.
- Скрипт предварительной обработки: `scripts/preprocessing/clean_accidents.py`.
- Итоговый каталог: `hdfs://namenode:8020/data/curated/accidents/`.

Подготовленный командой набор содержит **7 728 394 строки и 51 столбец**. Данные сохранены в формате Parquet с разбиением по годам (`year=...`).

**Parquet не хранится в GitHub.** Папку с готовыми файлами можно скачать по ссылке: https://drive.google.com/drive/folders/1btWE2M_klagyz7255X3gNLtkHZA8F4dt?usp=sharing

#### Вариант А. Загрузка готового Parquet (рекомендуется)

Этот вариант позволяет использовать уже обработанные данные без повторного запуска очистки исходного CSV.

**1. Получите готовую папку `accidents`.**

Скачайте папку целиком, включая каталоги `year=...` и файлы `.parquet`. Разместите её в корне проекта:

```text
us-accidents-bigdata/
└── data/
    └── accidents/
        ├── year=2016/
        ├── year=2017/
        └── ...
```

Если данные переданы в ZIP-архиве, предварительно распакуйте его, сохранив структуру каталогов.

**2. Проверьте наличие папки на компьютере.**

```bash
ls data/accidents
```

**3. Скопируйте папку в контейнер NameNode.**

```bash
docker compose cp data/accidents namenode:/tmp/
```

**4. Загрузите Parquet в HDFS.**

```bash
docker compose exec namenode \
  hdfs dfs -mkdir -p /data/curated
```

```bash
docker compose exec namenode \
  hdfs dfs -put /tmp/accidents /data/curated/
```

В результате данные будут доступны по пути:

```text
hdfs://namenode:8020/data/curated/accidents/
```

**Важно:** загрузку выполняйте только в том случае, если каталог `/data/curated/accidents/` ещё не существует. Не удаляйте и не перезаписывайте существующие данные без согласования с командой.

#### Вариант Б. Обработка исходного CSV с нуля

Если готовые Parquet-файлы недоступны, можно самостоятельно выполнить обработку исходного датасета.

1. Скачайте `US_Accidents_March23.csv` и загрузите его в HDFS согласно разделу 4.
2. Откройте `scripts/exploration/data_exploration.ipynb` в среде Jupyter/PySpark с доступом к Spark и HDFS.
3. Выполните этапы очистки и преобразования данных.
4. Сохраните результат в формате Parquet по пути `/data/curated/accidents/`.

В текущей конфигурации Docker Compose автоматический запуск этого ноутбука отдельной командой не предусмотрен. Скрипт `clean_accidents.py` не следует считать полной заменой ноутбука без проверки его функциональности.

#### Проверка подготовленных данных

Независимо от выбранного варианта проверьте наличие файлов в HDFS:

```bash
docker compose exec namenode \
  hdfs dfs -ls /data/curated/accidents
```

Затем запустите проверочный PySpark-скрипт:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  /opt/spark/work/validation/check_curated.py
```

**Ожидаемый результат:**

- Количество записей — **7 728 394**.
- Количество столбцов — **51**.
- Присутствуют признаки `year`, `hour`, `State`, `Weather_Condition`, `Severity`, `is_weekend`, `Sunrise_Sunset`.

**Примечание:** если необходимо только просмотреть готовый дашборд Superset, загружать Parquet в HDFS не требуется. Достаточно восстановить аналитическую базу из `exports/analytics.sql` и импортировать `exports/superset_dashboard_safe.zip`.

## 6. Обработка данных, ML и гипотезы

Исследование, расчёты ML и статистические проверки выполнены командой с использованием PySpark. Для демонстрации в Superset их **итоговые результаты** записаны в PostgreSQL (`ml_metrics`, `hypothesis_results`). Экспорт `analytics.sql` содержит эти результирующие таблицы, **но не заменяет код обучения модели и статистических тестов**.

В текущей таблице `hypothesis_results` отражены три проверки:

1. Различие распределения ДТП по часам в будни и выходные: χ² = 267277,52; p < 0,001; V Крамера = 0,186.
2. Связь видимости с тяжестью ДТП (Severity 3–4): p = 0,002; V Крамера = 0,0012 (крайне слабая связь).
3. Связь погодных условий с тяжестью ДТП: χ² = 346498,71; p < 0,001; V Крамера = 0,1236.

ML-результаты относятся к модели Random Forest: accuracy 0,7832; macro-F1 0,6135; ROC-AUC 0,7007; precision (класс 1) 0,4030; recall (класс 1) 0,3211; порог классификации 0,225. Для воспроизводимости обучения и проверки метрик нужен исходный ML-код команды; в описанных здесь скриптах аналитической выгрузки он отсутствует.

**Отличия от первоначального технического плана:** вместо заранее предложенной проверки «день/ночь и тяжесть ДТП» в текущих результатах содержится проверка «видимость и тяжесть ДТП». Исходный план также предполагал многоклассовую ML-классификацию Severity (1–4), а представленные метрики для `class 1` и порог 0,225 требуют уточнения фактической постановки задачи перед итоговой сдачей. Не следует объявлять эти пункты выполненными без подтверждения участников.

## 7. Аналитические таблицы PostgreSQL

Далее предусмотрены **два альтернативных пути**. Для быстрого развёртывания готового дашборда рекомендуется **7.1**; для пересчёта статистики из Parquet — **7.2**. Не нужно выполнять оба пути подряд.

Перед пересчётом можно проверить JDBC-подключение Spark к PostgreSQL:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  /opt/spark/work/tests/test_spark_postgres.py
```

Ожидаемый результат проверки — `connection_ok = 1`. JDBC-драйвер PostgreSQL подключается через `config/spark/spark-defaults.conf`.

### 7.1. Вариант А — восстановить готовые результаты (рекомендуется)

На **чистой** базе `analytics` выполните из корня проекта:

```bash
docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U postgres -d analytics \
  < exports/analytics.sql
```

Файл `exports/analytics.sql` создан с помощью `pg_dump --schema=public --no-owner --no-privileges`: он содержит **5 таблиц с агрегатами и результатами** и **1 представление VIEW**. Это **не резервная копия исходного CSV**, других баз или всех служебных объектов PostgreSQL.

**Не выполняйте импорт в уже заполненную базу:** команды `CREATE TABLE` могут конфликтовать с существующими объектами. При повторном развёртывании используйте отдельную пустую базу или заранее согласованный процесс её очистки с резервным копированием.

### 7.2. Вариант Б — пересчитать статистику из Parquet

Этот вариант требует готовых очищенных данных в HDFS (см. раздел 5). Запускайте команды по очереди:

**По штатам:**

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  /opt/spark/work/analytics/build_state_analytics.py
```

**По погодным условиям:**

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  /opt/spark/work/analytics/build_weather_analytics.py
```

**По времени суток:**

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.executor.memory=1g \
  --conf spark.cores.max=2 \
  /opt/spark/work/analytics/build_time_analytics.py
```

JDBC-драйвер PostgreSQL подключается через `config/spark/spark-defaults.conf`. Эти три скрипта создают/перезаписывают три агрегированные таблицы, **но не восстанавливают сами по себе** `ml_metrics`, `hypothesis_results` и `accidents_hourly_comparison`. Для полного дашборда потребуются результаты остальных этапов либо восстановление полного `analytics.sql` в чистую базу.

### 7.3. Проверка аналитической базы

```bash
docker compose exec -T postgres \
  psql -U postgres -d analytics -c '\dt public.*'

docker compose exec -T postgres \
  psql -U postgres -d analytics -c '\dv public.*'
```

Ожидаются:

| Объект | Назначение |
|---|---|
| `accidents_by_state_year` | ДТП по году и штату |
| `accidents_by_weather_year` | ДТП и доля Severity 3–4 по погодным условиям |
| `accidents_by_time` | ДТП по году, часу, типу дня и дню/ночи |
| `accidents_hourly_comparison` (VIEW) | Нормированные доли ДТП по часам для будней и выходных (за весь период) |
| `hypothesis_results` | Три статистические гипотезы |
| `ml_metrics` | Пять метрик Random Forest |

Проверка совпадения общего количества ДТП по агрегатам:

```bash
docker compose exec -T postgres \
  psql -U postgres -d analytics -c \
  'SELECT SUM(accident_count) AS total_accidents FROM public.accidents_by_state_year;'
```

Ожидается **7 728 394**. Для остальных двух таблиц с агрегатами сумма `accident_count` за все годы также должна совпадать. В представлении `accidents_hourly_comparison` для каждой группы `is_weekend` сумма `hour_share_pct` должна составлять примерно 100 % (с учётом округления).

### 7.4. Как обновить SQL-экспорт после изменения аналитики

```bash
mkdir -p exports
docker compose exec -T postgres \
  pg_dump -U postgres -d analytics \
  --schema=public --no-owner --no-privileges \
  > exports/analytics.sql
```

Экспорт можно проверить командой:

```bash
grep -E '^CREATE (TABLE|VIEW) public\.' exports/analytics.sql
```

## 8. Визуализация в Apache Superset

1. Перейдите на <http://localhost:8088> и войдите под администратором.
2. Убедитесь, что таблицы PostgreSQL восстановлены (раздел 7).
3. Импортируйте `exports/superset_dashboard_safe.zip` через функцию **Import dashboards** в разделе **Dashboards** (положение команды зависит от версии Superset).
4. При импорте/настройке подключения **US Accidents Analytics** укажите PostgreSQL `postgres:5432`, базу `analytics`, пользователя `postgres` и пароль `POSTGRES_PASSWORD` из `.env`. В безопасном ZIP пароль в SQLAlchemy URI отсутствует и должен вводиться отдельно.
5. Откройте дашборд **«US Accidents — Аналитический дашборд»**, убедитесь в наличии 8 визуализаций и фильтра **«Год»**.

Состав дашборда:

| Элемент | Данные | Фильтр «Год» |
|---|---|---|
| Общее количество ДТП (KPI) | `accidents_by_state_year` | Да |
| Топ-10 штатов | `accidents_by_state_year` | Да |
| Доля Severity 3–4 по погоде | `accidents_by_weather_year` | Да |
| ДТП днём и ночью | `accidents_by_time` | Да |
| ДТП по часам суток | `accidents_by_time` | Да |
| Будни и выходные по часам | `accidents_hourly_comparison` | Нет, доли за весь период |
| Метрики Random Forest | `ml_metrics` | Нет, метрики за общий тестовый набор |
| Результаты гипотез | `hypothesis_results` | Нет, результаты за весь период |

**Проверка:** при снятом фильтре KPI = **7 728 394**; при выборе 2022 года KPI = **1 762 452**. Графики ML, гипотез и сравнения будней/выходных не должны изменяться от фильтра года. Проценты по погоде показывают долю тяжёлых ДТП *внутри соответствующего погодного условия*, а не долю этой погоды среди всех ДТП. Доля дневных/ночных ДТП рассчитывается только для записей с известным значением Day/Night.

Для отчёта сохраните общий скриншот дашборда без фильтра и отдельный скриншот с выбранным годом. Экспорт Superset содержит настройки дашборда, **но не данные PostgreSQL**.

## 9. Совместная работа и Git

В репозиторий передаются код, Docker Compose, README и проверенные конфигурации. `exports/analytics.sql` и проверенная копия экспорта Superset могут передаваться через Git только после проверки на секреты и соответствия `.gitignore`.

Не публикуйте `.env`, оригинальные ZIP-файлы с паролями, локальный CSV, очищенные Parquet и данные Docker volumes. Проверяйте изменения перед коммитом (`git status`, `git diff --cached`). Для работы команды используйте отдельные ветки `feature/...` и Pull Request.

### Управление контейнерами

```bash
docker compose down      # Остановка без удаления volumes
docker compose up -d     # Повторный запуск
docker compose ps        # Состояние сервисов
```

**Не выполняйте `docker compose down -v` без полного понимания последствий:** эта команда удаляет тома и может уничтожить данные HDFS и PostgreSQL.
