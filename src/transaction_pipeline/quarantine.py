
from delta.tables import DeltaTable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_quarantine_records(
    quarantine_df: DataFrame,
    batch_id: str,
    pipeline_name: str,
) -> DataFrame:
    """
    Prepare invalid records for the quarantine Delta table.

    A deterministic fingerprint is created from the
    source record.

    The fingerprint allows the same invalid record to be
    processed again without creating duplicate quarantine
    rows.
    """

    # -----------------------------------------------------
    # CREATE A DETERMINISTIC RECORD FINGERPRINT
    # -----------------------------------------------------
    #
    # We convert every business/source field to STRING
    # before hashing.
    #
    # NULL values are explicitly represented as <NULL>.
    # Without this, null handling could make the fingerprint
    # ambiguous.
    #
    # Notice that batch_id is NOT included in the hash.
    #
    # This means that if the same rejected source record is
    # processed again during a later retry/run, it still has
    # the same fingerprint.

    record_fingerprint = F.sha2(
        F.concat_ws(
            "||",

            F.coalesce(
                F.col("transaction_id").cast("string"),
                F.lit("<NULL>")
            ),

            F.coalesce(
                F.col("customer_id").cast("string"),
                F.lit("<NULL>")
            ),

            F.coalesce(
                F.col("transaction_date").cast("string"),
                F.lit("<NULL>")
            ),

            F.coalesce(
                F.col("country").cast("string"),
                F.lit("<NULL>")
            ),

            F.coalesce(
                F.col("quantity").cast("string"),
                F.lit("<NULL>")
            ),

            F.coalesce(
                F.col("unit_price").cast("string"),
                F.lit("<NULL>")
            ),

            F.coalesce(
                F.col("updated_at").cast("string"),
                F.lit("<NULL>")
            ),
        ),
        256,
    )

    prepared_df = (
        quarantine_df

        # Generate the deterministic record identifier.
        .withColumn(
            "record_fingerprint",
            record_fingerprint
        )

        # Record which pipeline batch first quarantined it.
        .withColumn(
            "batch_id",
            F.lit(batch_id)
        )

        .withColumn(
            "pipeline_name",
            F.lit(pipeline_name)
        )

        # Record when the quarantine write occurred.
        .withColumn(
            "quarantined_at",
            F.current_timestamp()
        )

        # Select only columns belonging in the
        # quarantine Delta table.
        .select(
            "record_fingerprint",
            "batch_id",
            "pipeline_name",
            "transaction_id",
            "customer_id",
            "transaction_date",
            "country",
            "quantity",
            "unit_price",
            "updated_at",
            "rejection_reason",
            "quarantined_at",
        )

        # Protect against identical duplicate source records
        # appearing more than once in the same batch.
        .dropDuplicates(
            ["record_fingerprint"]
        )
    )

    return prepared_df


def merge_to_quarantine(
    spark,
    source_df: DataFrame,
    target_table: str,
) -> None:
    """
    Merge rejected records into the quarantine Delta table.

    Existing fingerprints are ignored.

    New rejected records are inserted.

    This makes repeated processing of the same source
    records retry-safe for this design.
    """

    target = DeltaTable.forName(
        spark,
        target_table
    )

    (
        target
        .alias("target")

        .merge(
            source_df.alias("source"),

            """
            target.record_fingerprint
            =
            source.record_fingerprint
            """
        )

        # We deliberately do not update an existing rejected
        # record when it is encountered again.
        #
        # The first quarantine event remains the recorded
        # event for this simple design.
        .whenNotMatchedInsertAll()

        .execute()
    )