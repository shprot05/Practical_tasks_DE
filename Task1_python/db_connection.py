import psycopg2

from settings import load_settings, ENV_FILE_PATH


def get_connection():
    settings = load_settings(ENV_FILE_PATH)
    conn = psycopg2.connect(
        host=settings.postgres.host,
        user=settings.postgres.user,
        password=settings.postgres.password.get_secret_value(),
        port=settings.postgres.port,
        dbname=settings.postgres.db,
    )
    return conn
