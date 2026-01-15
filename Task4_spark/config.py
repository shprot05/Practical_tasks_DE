import os
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver",
}

POSTGRES_JAR = "org.postgresql:postgresql:42.7.8"


TABLES_TO_LOAD = [
    "actor", "address", "category", "city", "country",
    "customer", "film", "film_actor", "film_category",
    "inventory", "language", "payment", "rental",
    "staff", "store"
]

def setup_environment():
    if os.name == 'nt':
        os.environ['HADOOP_HOME'] = 'C:\\hadoop'