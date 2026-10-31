from pyspark.sql import DataFrame

def read_delta_table(spark, table_name:str) -> DataFrame:
    """
    Read a Databricks Delta table and return it as a spark DataFrame.

    Parameters
    spark:
        Active Spark session connected to Databricks.
    table_name:
        Fully qualified Databricks table name
    Returns
    DataFrame
        Spark DataFrame representing the requested table
    """
    # spark.table() reads a registered Databricks table
    df = spark.table(table_name)

    # Return the DataFrame
    return df