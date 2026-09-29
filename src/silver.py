# Import Spark DataFrame
from pyspark.sql import DataFrame

# Import Spark SQL Functions
from pyspark.sql import functions  as F

# Import Windows so we can deterministically select
# the latest version of each transaction
from pyspark.sql.window import Window

# Import DeltaTable to prform Delta MERGE Operations
from delta.tables import DeltaTable


def deduplicate_latest_transactions(df:DataFrame) -> DataFrame:
    """
    Keep only the latest version of each transaction.

    We assume:
    transaction_id = business key
    updated_at     = source version timestamp

    If multiple versions of the same transaction arrive,
    the row with the newest updated_at is retained.
    """

    # Define a window for each transaction
    # within each transaction_id, sort newest updated_at
    latest_window = (
        Window
        .partitionBy("transaction_id")
        .orderBy(F.col("updated_at").desc())
    )

    # Assign row number 1 to latest version
    ranked_df = (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(latest_window)
        )
    )

    # Keep only the latest version of each transaction
    latest_df = (
        ranked_df
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    return latest_df


def merge_to_silver(spark,source_df: DataFrame,target_table: str) -> None:
    """
    Merge transaction records into the Silver Delta table.

    Behaviour:
        - Existing transaction + newer source version → UPDATE
        - Existing transaction + older/same version  → IGNORE
        - New transaction                            → INSERT

    Parameters
    ----------
    spark:
        Active Spark session connected to Databricks.
    source_df:
        Cleaned and deduplicated incoming transactions.
    target_table:
        Fully qualified name of the target Silver Delta table.
    """

    # Reference the existing Delta table.
    silver_target = DeltaTable.forName(spark,target_table)

    # Merge source transactions into the target table.
    (
        silver_target
        .alias("target")
        .merge(
            source_df.alias("source"),
            # transaction_id is the Silver business key.
            "target.transaction_id = source.transaction_id"
        )

        # Only overwrite an existing Silver record when
        # the incoming source version is newer.
        .whenMatchedUpdateAll(
            condition="""
                source.updated_at > target.updated_at
            """
        )

        # Insert transactions that do not yet exist in Silver.
        .whenNotMatchedInsertAll()

        # Execute the Delta transaction.
        .execute()
    )