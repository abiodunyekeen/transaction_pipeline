# Import spark SQL functions for column expression
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_validation_columns(df:DataFrame, allowed_countries: list[str]) -> DataFrame:
    """
        Add validation columns to a transaction DataFrame.

        The function does not immediately remove invalid rows.
        Instead, it adds:
            - is_valid
            - rejection_reason

        This allows us to separate valid records from
        quarantined records later.
        """
    #transaction_id is required because it identifies the logical transaction
    transaction_id_valid = (F.col("transaction_id").isNotNull())

    # quantity must exist and greater than zero
    quantity_valid = (F.col("quantity").isNotNull() & (F.col("quantity") > 0))

    #unit_price must exist and greater than zero
    unit_price_valid = (F.col("unit_price").isNotNull() & (F.col("unit_price") > 0))

    # Country must be one of the values accepted by the current business rule
    country_valid = (F.col("country").isNotNull() & (F.col("country").isin(allowed_countries)))

    # Combine all the individual business rules.
    is_valid = (
        transaction_id_valid
        & quantity_valid
        & unit_price_valid
        & country_valid
    )

    return (
        df
        .withColumn("is_valid", is_valid)
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                "; ",
                F.when(~transaction_id_valid, F.lit("transaction_id missing")),
                F.when(~quantity_valid, F.lit("quantity must be greater than zero")),
                F.when(~unit_price_valid, F.lit("unit price must be greater than zero")),
                F.when(~country_valid, F.lit("country not allowed"))
            )
                    )
    )

def get_valid_records(df:DataFrame) -> DataFrame:
    """
    Return only records that passed validation.
    """
    return df.filter(F.col("is_valid") == True)

def get_quarantine_records(df:DataFrame) -> DataFrame:
    """
    Return only records that failed validation
    """
    return df.filter(F.col("is_valid") == False)
