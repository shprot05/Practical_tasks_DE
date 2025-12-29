from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config import logger


def run_category_film_count(dfs):
    logger.info("Query 1: Films by Category")
    category_df = dfs['category']
    film_category_df = dfs['film_category']

    result = category_df.join(film_category_df, "category_id") \
        .groupby("name") \
        .agg(F.count("film_id").alias("film_count")) \
        .orderBy(F.col("film_count").desc())

    result.show()


def run_actor_film_count(dfs):
    logger.info("Query 2: Top Actors by Film Count")
    actor_df = dfs['actor']
    film_actor_df = dfs['film_actor']

    result = film_actor_df.join(actor_df, "actor_id") \
        .groupby("actor_id", "first_name", "last_name") \
        .agg(F.count("film_id").alias("film_count")) \
        .orderBy(F.col("film_count").desc())

    result.show(10)


def run_revenue_by_category(dfs):
    logger.info("Query 3: Revenue by Category")
    result = dfs['category'] \
        .join(dfs['film_category'], "category_id") \
        .join(dfs['inventory'], "film_id") \
        .join(dfs['rental'], "inventory_id") \
        .join(dfs['payment'], "rental_id") \
        .groupBy("name") \
        .agg(F.sum("amount").alias("amount_sum")) \
        .orderBy(F.col("amount_sum").desc())

    result.show(3)


def run_missing_inventory(dfs):
    logger.info("Query 4: Films not in Inventory")
    result = dfs['film'].alias("f") \
        .join(dfs['inventory'].alias("i"), "film_id", "left") \
        .filter(F.col("i.inventory_id").isNull()) \
        .select("f.film_id", "f.title") \
        .orderBy("f.title")

    result.show(truncate=False)


def run_top_actors_in_children(dfs):
    logger.info("Query 5: Top Actors in Children Category (handling ties)")

    joined_df = dfs['actor'] \
        .join(dfs['film_actor'], "actor_id") \
        .join(dfs['film'], "film_id") \
        .join(dfs['film_category'], "film_id") \
        .join(dfs['category'], "category_id")

    agg_df = joined_df \
        .filter(F.col("name") == "Children") \
        .groupBy("actor_id", "first_name", "last_name") \
        .agg(F.count("film_id").alias("film_count"))

    window_spec = Window.orderBy(F.col("film_count").desc())

    result = agg_df \
        .withColumn("rank", F.dense_rank().over(window_spec)) \
        .filter(F.col("rank") <= 3) \
        .select("first_name", "last_name", "film_count", "rank")

    result.show()


def run_active_customers_by_city(dfs):
    logger.info("Query 6: Active/Inactive Customers by City")
    joined_df = dfs['customer'] \
        .join(dfs['address'], "address_id") \
        .join(dfs['city'], "city_id")

    result = joined_df \
        .groupby("city_id", "city") \
        .agg(
        F.sum(F.when(F.col("active") == 1, 1).otherwise(0)).alias("active_customers"),
        F.sum(F.when(F.col("active") == 0, 1).otherwise(0)).alias("inactive_customers")
    ) \
        .orderBy(F.col("inactive_customers").desc())

    result.show()


def run_complex_city_analysis(dfs):
    logger.info("Query 7: Complex City Analysis")
    base_df = dfs['category'] \
        .join(dfs['film_category'], "category_id") \
        .join(dfs['film'], "film_id") \
        .join(dfs['inventory'], "film_id") \
        .join(dfs['rental'], "inventory_id") \
        .join(dfs['customer'], "customer_id") \
        .join(dfs['address'], "address_id") \
        .join(dfs['city'], "city_id")

    df_starts_with_a = base_df \
        .filter(F.col("city").like("a%")) \
        .groupBy("name") \
        .agg(F.count("rental_id").alias("rentals_count")) \
        .orderBy(F.col("rentals_count").desc()) \
        .limit(1) \
        .withColumn("group_name", F.lit("Cities starting with 'a'")) \
        .select("group_name", "name", "rentals_count")


    df_with_hyphen = base_df \
        .filter(F.col("city").like("%-%")) \
        .groupBy("name") \
        .agg(F.count("rental_id").alias("rentals_count")) \
        .orderBy(F.col("rentals_count").desc()) \
        .limit(1) \
        .withColumn("group_name", F.lit("Cities with '-'")) \
        .select("group_name", "name", "rentals_count")

    result_union = df_starts_with_a.union(df_with_hyphen)
    result_union.show(truncate=False)