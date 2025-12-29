from config import DB_URL, DB_PROPERTIES, TABLES_TO_LOAD, logger


def load_data(spark):
    data_frames = {}

    logger.info(f"Attempting to connect to database at {DB_URL}")

    try:
        spark.read.jdbc(url=DB_URL, table=TABLES_TO_LOAD[0], properties=DB_PROPERTIES).limit(1).collect()
        logger.info("SUCCESS! Database connection established.")

    except Exception as e:
        logger.error("CONNECTION ERROR: Could not connect to the database.")
        raise e

    logger.info(f"Loading {len(TABLES_TO_LOAD)} tables...")
    for table in TABLES_TO_LOAD:
        try:
            df = spark.read.jdbc(url=DB_URL, table=table, properties=DB_PROPERTIES)
            data_frames[table] = df
            logger.debug(f"Loaded table: {table}")
        except Exception as e:
            logger.warning(f"Failed to load table {table}: {e}")

    return data_frames