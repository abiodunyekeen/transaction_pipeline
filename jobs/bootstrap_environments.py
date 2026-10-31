import os

from src.transaction_pipeline.spark_session import get_spark_session

from src.transaction_pipeline.config import get_config

# Logging.
from src.transaction_pipeline.logging_utils import get_logger

from src.transaction_pipeline.table_setup import create_pipeline_tables

# Determine which environment is being created
environment = os.getenv("PIPELINE_ENV", "DEV")

# get config data
config = get_config(environment)

# Create one logger for this pipeline.
logger = get_logger(config["pipeline_name"])

def create_environment_schemas(spark):
    """
    Create DEV, TEST, STAGING and PROD schemas.
    Create Schema IF NOT EXISTS  makes this operation safe to run repeatedly
    """

    environments = [
        "dev",
        "test",
        "staging",
        "prod"
    ]

    existing = {row["databaseName"] for row in spark.sql("SHOW SCHEMAS IN workspace").collect()}

    for environment in environments:
        schema_name = f"workspace.{environment}"

        if environment in existing:
            logger.info("Schema %s already exists", schema_name)
        else:
            logger.info("Creating schema %s ", schema_name)
            spark.sql(f""" CREATE SCHEMA IF NOT EXISTS {schema_name} """)
            logger.info("Schema %s  successfully created.", schema_name)



def main():

    # Connect to Databricks serverless
    spark = get_spark_session()

    # Create environment schema
    create_environment_schemas(spark)

    # Create environment tables
    create_pipeline_tables(spark,config)

    logger.info(" %s environment ready.",config['environment'])






if __name__ == "__main__":
    main()