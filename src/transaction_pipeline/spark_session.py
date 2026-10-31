# Import DatabricksSession so our local Pycharm code
#can connect to Databricks serverless compute
from databricks.connect import DatabricksSession

def get_spark_session():
    """
    Create and return spark session connected to
    Databricks serverless compute

    Keeping this logic in one function means the rest
    of the project does not need to know how the Spark
    connection is configured.
    """

    # Create a spark session backed by Databricks serverless.
    spark = (
        DatabricksSession
        .builder
        .serverless()
        .getOrCreate()
    )

    # Return the session so other module can use it
    return spark