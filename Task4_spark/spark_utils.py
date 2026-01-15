from pyspark.sql import SparkSession
from config import POSTGRES_JAR, setup_environment


def get_spark_session(app_name="PagilaConnectionTest"):
    setup_environment()

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", POSTGRES_JAR)
        .getOrCreate()
    )
    return spark