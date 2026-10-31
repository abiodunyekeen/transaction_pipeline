# Import DeltaTable for delta Merge Operation
from delta.tables import DeltaTable

# Import Spark SQL function
from pyspark.sql import functions as F


def create_control_table(spark,table_name:str)-> None:
    """
    Create the pipeline control table if it does not exist.
    The table records the execution status of each batch.
    """
    # The table contains one logical row per
    # pipeline_name + batch_id.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
        batch_id STRING,
        pipeline_name STRING,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        status STRING,
        error_message STRING
        ) USING DELTA
        """
    )


def mark_batch_started(spark,
                       control_table_name:str,
                       pipeline_name:str,
                       batch_id:str)->None:
    """
    Record that a pipeline has started
    """
    # Get the batch_id & pipeline name
    data=[(batch_id,pipeline_name)]
    # Columns
    columns=["batch_id","pipeline_name"]

    # Create a one-row source DataFrame representing
    # the current pipeline batch.
    source_df = (
        spark.createDataFrame(data,columns)
        # Mark the batch as currently processing.
        .withColumn("status",F.lit("PROCESSING"))
        # Record when processing started.
        .withColumn("started_at",F.current_timestamp())
        # A running batch has not completed yet.
        .withColumn("completed_at",F.lit(None).cast("timestamp"))
        # No error has occurred yet.
        .withColumn("error_message", F.lit(None).cast("string"))
    )

    # Reference the Delta control table.
    target = DeltaTable.forName(spark,control_table_name)

    # Upsert the batch record.
    (
        target.alias("target")
    .merge(
        source_df.alias("source"),
        """
        target.batch_id == source.batch_id
        AND
        target.pipeline_name == source.pipeline_name
        """
    )
        # If this batch already exists because of a retry,
        # reset its status to PROCESSING.
     .whenMatchedUpdate(
            set={
                "status":"source.status",
                "started_at":"source.started_at",
                "completed_at":"source.completed_at",
                "error_message":"source.error_message"
            }
        )
        # Otherwise create a new control-table row.
    .whenNotMatchedInsertAll()
    .execute()

    )


def mark_batch_success(spark,
                       control_table_name:str,
                       pipeline_name:str,
                       batch_id:str)->None:
    """
     Mark a completed pipeline batch as successful.
    """
    # Reference the Delta control table
    target = DeltaTable.forName(spark,control_table_name)

    # Update only the matching pipeline batch.
    target.update(
        condition=f"""
        batch_id == '{batch_id}'
        AND pipeline_name == '{pipeline_name}'
        """,
        set={
            # The complete pipeline succeeded.
            "status": F.lit("SUCCESS"),
            # Record completion time.
            "completed_at": F.current_timestamp(),
            # Successful runs should have no error.
            "error_message": F.lit(None).cast("string")
        }
    )


def mark_batch_failed(spark,
                       control_table_name:str,
                       pipeline_name:str,
                       batch_id:str,
                      error_message)->None:
    """
    Mark a pipeline batch as failed and record
    the error message.
    """

    # Reference the Delta control table.
    target = DeltaTable.forName(spark,control_table_name)

    # Escape single quotes so the error message does not
    # break the SQL condition/expression.
    safe_error_message= error_message.replace("'","''")

    # Update the failed batch.
    target.update(
        condition=f"""
           batch_id == '{batch_id}'
           AND pipeline_name == '{pipeline_name}'
           """,
        set={
            # Mark this batch as failed.
            "status": F.lit("FAILED"),
            # Record when the failure occur
            "completed_at": F.current_timestamp(),
            # Preserve the failure message for troubleshooting.
            "error_message": F.lit(safe_error_message)
        }
    )