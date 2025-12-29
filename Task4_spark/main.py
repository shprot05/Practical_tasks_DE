import sys
from spark_utils import get_spark_session
from data_loader import load_data
import queries
from config import logger


def main():
    logger.info("Starting Application...")

    spark = get_spark_session()
    logger.info("Spark session created successfully.")

    try:
        dfs = load_data(spark)

        queries.run_category_film_count(dfs)
        queries.run_actor_film_count(dfs)
        queries.run_revenue_by_category(dfs)
        queries.run_missing_inventory(dfs)
        queries.run_top_actors_in_children(dfs)
        queries.run_active_customers_by_city(dfs)
        queries.run_complex_city_analysis(dfs)

    except Exception as e:
        logger.error(f"Critical execution error: {e}", exc_info=True)

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()