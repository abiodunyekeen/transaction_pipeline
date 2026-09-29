# Import the spark DataFrame
from pyspark.sql import DataFrame

# Import Spark SQL functions
from pyspark.sql import functions as F


def calculate_transaction_amount(df:DataFrame)-> DataFrame:
    """
    Add the transaction amount to each row.
    Business rule:
        amount = quantity * unit_price
    Parameters
    ----------
    df:
        Input transaction DataFrame.
    Returns
    -------
    DataFrame
        Original DataFrame with an additional
        'amount' column.
    """

    # Calculate the monetary value of each transaction.
    return (
        df
        .withColumn(
            "total_amount",
            (F.col("quantity")) * (F.col("unit_price"))
        )
    )