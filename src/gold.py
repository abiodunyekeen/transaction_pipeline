# Import Spark DatatFrame
from pyspark.sql import DataFrame
# Import Spark SQL functions for aggregation.
from pyspark.sql import functions as F
# Import DeltaTable so we can MERGE Gold results
# into an existing Delta table.
from delta.tables import DeltaTable

def build_daily_country_gold(silver_df:DataFrame)-> DataFrame:
    """
      Build the daily-country Gold aggregate.

      Gold grain:
          One row per (transaction_date, country)

      Metrics:
          - total_revenue
          - transaction_count
          - unique_customers
          - average_transaction_value

      Parameters
      ----------
      silver_df:
          Current clean Silver transaction DataFrame.

      Returns
      -------
      DataFrame
          Aggregated Gold DataFrame.
      """
    # Aggregate transaction-level Silver data
    gold_df = (
        silver_df
        .groupBy("transaction_date","country")
        .agg(
            # Sum the transaction amount for total revenue.
            F.sum("total_amount").alias("total_revenue"),
            # Count the number of transaction rows.
            F.count("*").alias("transaction_count"),
            # Count unique customers within each Gold group.
            F.countDistinct("customer_id").alias("unique_customer"),
            # Calculate the average transaction amount.
            F.avg("total_amount").alias("average_transaction_value")

        )
    )

    return gold_df


def merge_to_gold(spark, source_df:DataFrame, target_table:str)-> None:
    """
       Merge recalculated Gold groups into the Gold Delta table.

       Gold business key:
           transaction_date + country

       Existing groups are replaced with the latest
       authoritative aggregate values.

       New groups are inserted.
    """

    # Reference the existing Gold delta table
    gold_target = DeltaTable.forName(spark, target_table)

    (
        gold_target.alias("target")
        .merge(
            source_df.alias("source"),
            """
            target.transaction_date = source.transaction_date
            AND target.country = source.country
            """
        )
        # Replace existing Gold metrics with the
        # authoritative recomputed values.
        .whenMatchedUpdateAll()
        # Insert Gold groups that do not yet exist.
        .whenNotMatchedInsertAll()
        # Execute the Delta transaction.
        .execute()
    )