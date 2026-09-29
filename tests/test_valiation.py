import pytest

#Import the functions we want to test
from src.validation import add_validation_columns

@pytest.mark.spark
def test_valid_transaction_is_accepted(spark):
    """
    A transaction containing valid values should
    be marked as valid.
    """
    # Create a very small test dataset
    test_data = [
        (
            "TX001",
            "C001",
            2,
            25.0,
            "UK"
        )
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "quantity",
        "unit_price",
        "country"
    ]

    # Create the test DataFrame
    df = spark.createDataFrame(test_data,columns)

    # Run the validation function
    result_df = add_validation_columns(df=df, allowed_countries=["UK","US","NG","DE"])

    result = result_df.first()

    #Assert the expected behaviour
    assert result["is_valid"] is True

@pytest.mark.spark
def test_negative_quantity_is_rejected(spark):
    """
    A transaction with a negative quantity
    should fail validation.
    """

    # Deliberately create a bad transaction.
    test_data = [
        (
            "TX002",
            "C002",
            -3,
            50.0,
            "UK"
        )
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "quantity",
        "unit_price",
        "country"
    ]

    # Create the test DataFrame.
    df = spark.createDataFrame(
        test_data,
        columns
    )

    # Apply validation.
    result_df = add_validation_columns(
        df=df,
        allowed_countries=[
            "UK",
            "US",
            "NG",
            "DE"
        ]
    )

    # Retrieve the one test record.
    result = result_df.first()

    # Confirm validation failed
    assert result["is_valid"] is False

    # Confirm the rejection reason explain why
    assert(
        "quantity must be greater than zero"
        in result["rejection_reason"]
    )

@pytest.mark.spark
def test_negative_unit_price_is_rejected(spark):
    """
    A transaction with a negative unit Price
    should fail validation.
    """

    # Deliberately create a bad transaction.
    test_data = [
        (
            "TX002",
            "C002",
            30,
            -5.0,
            "UK"
        )
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "quantity",
        "unit_price",
        "country"
    ]

    # Create the test DataFrame.
    df = spark.createDataFrame(
        test_data,
        columns
    )

    # Apply validation.
    result_df = add_validation_columns(
        df=df,
        allowed_countries=[
            "UK",
            "US",
            "NG",
            "DE"
        ]
    )

    # Retrieve the one test record.
    result = result_df.first()

    # Confirm validation failed
    assert result["is_valid"] is False

    # Confirm the rejection reason explain why
    assert(
        "unit price must be greater than zero"
        in result["rejection_reason"]
    )

@pytest.mark.spark
def test_disallowed_country_is_rejected(spark):
    """
    A transaction with a country that not included
    should fail validation.
    """

    # Deliberately create a bad transaction.
    test_data = [
        (
            "TX002",
            "C002",
            30,
            50.0,
            "CA"
        )
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "quantity",
        "unit_price",
        "country"
    ]

    # Create the test DataFrame.
    df = spark.createDataFrame(
        test_data,
        columns
    )

    # Apply validation.
    result_df = add_validation_columns(
        df=df,
        allowed_countries=[
            "UK",
            "US",
            "NG",
            "DE"
        ]
    )

    # Retrieve the one test record.
    result = result_df.first()

    # Confirm validation failed
    assert result["is_valid"] is False

    # Confirm the rejection reason explain why
    assert(
        "country not allowed"
        in result["rejection_reason"]
    )