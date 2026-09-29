import os
import time
import uuid
# Import our configuration
from src.config import CONFIG

# Import the reusable Databricks spark connection
from src.spark_session import get_spark_session

# Import ingestion function
from src.ingestion import read_delta_table

# Import Validation functions
from src.validation import (
        add_validation_columns,
        get_valid_records,
        get_quarantine_records
)

# Import transformation functions
from src.transformations import calculate_transaction_amount

# Import Silver-layer functions.
from src.silver import (
    deduplicate_latest_transactions,
    merge_to_silver,
)

# Import Gold-layer functions.
from src.gold import (
    build_daily_country_gold,
    merge_to_gold,
)

# Logging.
from src.logging_utils import get_logger

# Pipeline control-table functions.
from src.control import (
    create_control_table,
    mark_batch_started,
    mark_batch_success,
    mark_batch_failed,
)

# Create one logger for this pipeline.
logger = get_logger(CONFIG["pipeline_name"])


def run_pipeline(spark,batch_id):
    """
    Execute one transaction-pipeline batch.
    Keeping this separate from main() makes the
    orchestration logic easier to test.
    """

    # --------------------------------------------
    # INGESTION
    # --------------------------------------------
    logger.info("Ingestion started | batch_id=%s", batch_id)

    source_df = read_delta_table(spark=spark, table_name=CONFIG["silver_table"])

    logger.info("Ingestion completed | batch_id=%s", batch_id)

    # --------------------------------------------
    # VALIDATION
    # --------------------------------------------
    logger.info("Validation started | batch_id=%s", batch_id)

    # Apply business validation rules
    validated_df = add_validation_columns(
        df=source_df,
        allowed_countries=CONFIG["allowed_countries"]
    )

    # Separate records that passed business rules.
    valid_df = get_valid_records(df=validated_df)

    # Preserve failed records for quarantine processing
    quarantine_df = get_quarantine_records(df=validated_df)

    logger.info("Validation completed | batch_id=%s", batch_id)

    # --------------------------------------------
    # TRANSFORMATION
    # --------------------------------------------
    # Calculate amount for only valid transactions
    transformed_df = calculate_transaction_amount(valid_df)

    # --------------------------------------------
    # SILVER
    # --------------------------------------------

    logger.info("Silver processing started | batch_id=%s", batch_id)

    # Keep the latest source version for each
    # logical transaction.
    latest_transactions_df = deduplicate_latest_transactions(transformed_df)

    # Merge the latest transaction state into Silver.
    merge_to_silver(
        spark=spark,
        source_df=latest_transactions_df,
        target_table=CONFIG["silver_table"]
    )

    logger.info("Silver processing completed | batch_id=%s", batch_id)

    # --------------------------------------------
    # GOLD
    # --------------------------------------------
    logger.info("Gold processing started | batch_id=%s", batch_id)

    # Reread the committed Silver state so Gold
    # is generated from authoritative Silver data.
    current_silver_df = read_delta_table(
        spark=spark,
        table_name=CONFIG["silver_table"]
    )

    # Build Gold metrics from current Silver.
    gold_df = build_daily_country_gold(current_silver_df)

    # Publish the Gold aggregate.
    merge_to_gold(
        spark=spark,
        source_df=gold_df,
        target_table=CONFIG["gold_table"]
    )

    logger.info("Gold processing completed | batch_id=%s", batch_id)


def main():
    """
    Run the complete transaction pipeline.
    """

    # Start measuring total pipeline duration.
    start_time = time.perf_counter()

    #Allow an orchestrator to supply a batch ID.
    # If no BATCH_ID is supplied during local development,
    # generate a unique one.
    batch_id = os.getenv("BATCH_ID", uuid.uuid4().hex)

    # Create the spark session connected to databricks serverless
    spark = get_spark_session()

    # Make sure the batch-control table exists.
    create_control_table(spark=spark, table_name=CONFIG["control_table"])

    # Record that this batch has begun
    mark_batch_started(spark=spark,
                       control_table_name=CONFIG["control_table"],
                       batch_id=batch_id,
                       pipeline_name=CONFIG["pipeline_name"]
                       )

    logger.info("Pipeline started | batch_id=%s",batch_id)

    try:

        run_pipeline(spark=spark,batch_id=batch_id)

        # --------------------------------------------
        # SUCCESS
        # --------------------------------------------

        # Only mark SUCCESS after all required stages
        # have completed successfully.
        mark_batch_success(spark=spark,
                           control_table_name=CONFIG["control_table"],
                           pipeline_name=CONFIG["pipeline_name"],
                           batch_id=batch_id
                           )

        # Calculate total execution duration.
        duration_seconds = (time.perf_counter() - start_time)

        logger.info("Pipeline completed successfully | batch_id=%s | duration_seconds=%s", batch_id, duration_seconds)
    except Exception as error:

        # Record the failure in our control table.
        mark_batch_failed(
            spark=spark,
            control_table_name=CONFIG["control_table"],
            pipeline_name=CONFIG["pipeline_name"],
            batch_id=batch_id,
            error_message=str(error)
        )

        # logger.exception() includes both the message
        # and the complete Python stack trace.
        logger.exception("Pipeline failed | batch_id=%s", batch_id)

        # Re-raise the exception so Databricks Jobs,
        # Airflow, CI/CD or another orchestrator can
        # correctly see that the job FAILED.
        raise













# Python executes main() only when this file is run directly
# as the job entry point.
if __name__ == "__main__":
    main()