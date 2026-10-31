
from datetime import date, datetime

from src.transaction_pipeline.config import get_config
from src.transaction_pipeline.spark_session import get_spark_session


def main():
    """
    Load a controlled dataset into the DEV Bronze table.

    This dataset intentionally contains:
    - valid records
    - invalid business values
    - duplicate transaction versions

    It allows us to observe exactly what the pipeline
    does in the DEV environment.
    """

    # -----------------------------------------------------
    # LOAD DEV CONFIGURATION
    # -----------------------------------------------------

    config = get_config("DEV")

    # Connect to Databricks Serverless.
    spark = get_spark_session()

    print(
        f"Seeding environment: "
        f"{config['environment']}"
    )

    # -----------------------------------------------------
    # RESET DEV TABLES
    # -----------------------------------------------------
    # We want this exercise to be deterministic.
    #
    # Every time we run this seed script we start with
    # empty DEV tables.

    spark.sql(
        f"TRUNCATE TABLE {config['source_table']}"
    )

    spark.sql(
        f"TRUNCATE TABLE {config['quarantine_table']}"
    )

    spark.sql(
        f"TRUNCATE TABLE {config['silver_table']}"
    )

    spark.sql(
        f"TRUNCATE TABLE {config['gold_table']}"
    )

    spark.sql(
        f"TRUNCATE TABLE {config['control_table']}"
    )

    # -----------------------------------------------------
    # CREATE CONTROLLED BRONZE DATA
    # -----------------------------------------------------

    data = [

        # -------------------------------------------------
        # Valid UK transaction.
        #
        # amount should eventually become:
        # 2 * 25 = 50
        # -------------------------------------------------
        (
            "TX001",
            "C101",
            date(2026, 9, 30),
            "UK",
            2,
            25.0,
            datetime(2026, 9, 30, 8, 0, 0),
        ),

        # -------------------------------------------------
        # Valid UK transaction.
        #
        # amount:
        # 1 * 100 = 100
        # -------------------------------------------------
        (
            "TX002",
            "C102",
            date(2026, 9, 30),
            "UK",
            1,
            100.0,
            datetime(2026, 9, 30, 8, 5, 0),
        ),

        # -------------------------------------------------
        # INVALID:
        # quantity must be greater than zero.
        # -------------------------------------------------
        (
            "TX003",
            "C103",
            date(2026, 9, 30),
            "US",
            -2,
            50.0,
            datetime(2026, 9, 30, 8, 10, 0),
        ),

        # -------------------------------------------------
        # INVALID:
        # unit_price must be greater than zero.
        # -------------------------------------------------
        (
            "TX004",
            "C104",
            date(2026, 9, 30),
            "NG",
            2,
            0.0,
            datetime(2026, 9, 30, 8, 15, 0),
        ),

        # -------------------------------------------------
        # INVALID:
        # FR is not currently in allowed_countries.
        # -------------------------------------------------
        (
            "TX005",
            "C105",
            date(2026, 9, 30),
            "FR",
            1,
            80.0,
            datetime(2026, 9, 30, 8, 20, 0),
        ),

        # -------------------------------------------------
        # Older version of TX006.
        # -------------------------------------------------
        (
            "TX006",
            "C106",
            date(2026, 9, 30),
            "DE",
            1,
            40.0,
            datetime(2026, 9, 30, 8, 25, 0),
        ),

        # -------------------------------------------------
        # Newer version of TX006.
        #
        # This should survive deduplication.
        #
        # amount:
        # 1 * 60 = 60
        # -------------------------------------------------
        (
            "TX006",
            "C106",
            date(2026, 9, 30),
            "DE",
            1,
            60.0,
            datetime(2026, 9, 30, 9, 0, 0),
        ),
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "transaction_date",
        "country",
        "quantity",
        "unit_price",
        "updated_at",
    ]

    # Create the Spark DataFrame.
    bronze_df = spark.createDataFrame(
        data,
        columns,
    )

    # -----------------------------------------------------
    # WRITE TO REAL DEV BRONZE DELTA TABLE
    # -----------------------------------------------------

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            config["source_table"]
        )
    )

    print(
        f"Loaded {bronze_df.count()} "
        f"rows into {config['source_table']}"
    )


if __name__ == "__main__":
    main()