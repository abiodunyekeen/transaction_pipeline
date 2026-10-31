
def create_pipeline_tables(spark, config: dict):
    """
    Create the Delta tables required by one environment.

    The same function is used for DEV, TEST,
    STAGING and PROD.
    """

    # Create Bronze
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS  {config["source_table"]}
        (
            transaction_id STRING,
            customer_id STRING,
            transaction_date DATE,
            country STRING,
            quantity INT,
            unit_price DOUBLE,
            updated_at TIMESTAMP
        )
        USING DELTA
        """
    )

    # Create Quarantine
    # Invalid source records are preserved here rather
    # than simply disappearing from the pipeline.
    #
    # record_fingerprint gives each rejected source record
    # a deterministic identifier that helps make retries
    # idempotent.

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
            {config["quarantine_table"]}
        (
            record_fingerprint STRING,

            batch_id STRING,
            pipeline_name STRING,

            transaction_id STRING,
            customer_id STRING,
            transaction_date DATE,
            country STRING,
            quantity INT,
            unit_price DOUBLE,
            updated_at TIMESTAMP,

            rejection_reason STRING,

            quarantined_at TIMESTAMP
        )
        USING DELTA
        """
    )

    # Create Silver
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config["silver_table"]}
        (
            transaction_id STRING,
            customer_id STRING,
            transaction_date DATE,
            country STRING,
            quantity INT,
            unit_price DOUBLE,
            updated_at TIMESTAMP,
            amount DOUBLE
        )
        USING DELTA
        """
    )


    # Create Gold
    spark.sql(
        f"""
           CREATE TABLE IF NOT EXISTS {config["gold_table"]}
           (
                transaction_date DATE,
                country STRING,
                total_revenue DOUBLE,
                transaction_count BIGINT,
                unique_customers BIGINT,
                average_transaction_value DOUBLE
           )
           USING DELTA
           """
    )

    # Create Control_table
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS
                {config["control_table"]}
            (
                batch_id STRING,
                pipeline_name STRING,
                status STRING,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                error_message STRING
            )
            USING DELTA
            """
    )