# Import pytest so we can create reusable test fixtures.
import pytest

# Import our Databricks Spark-session factory
from src.spark_session import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """
    Create one Spark session for the entire pytest session.

    scope="session" means pytest creates this fixture once,
    then all Spark-based tests can reuse it.
    """

    # Connect to Databricks serverless
    spark_session = get_spark_session()

    # yield makes the session available to the tests.
    yield spark_session

