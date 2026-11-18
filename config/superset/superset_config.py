
import os
from sqlalchemy.engine import URL

# Секретный ключ Superset
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Подключение к базе метаданных PostgreSQL
SQLALCHEMY_DATABASE_URI = URL.create(
    drivername="postgresql+psycopg2",
    username="postgres",
    password=os.environ["POSTGRES_PASSWORD"],
    host="postgres",
    port=5432,
    database="superset_meta",
).render_as_string(hide_password=False)
