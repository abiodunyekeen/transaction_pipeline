import pytest

#Import the functions we want to test
from src.validation import add_validation_columns,get_validation_metrics

@pytest.mark.spark
def test_validation_metrics_are_correct(spark):
    """
    Verify that validation metrics correctly count
    total, valid and invalid records.
    """

    # Create a tiny controlled dataset.
    test_data = [
        ("TX001", True),
        ("TX002", True),
        ("TX003", False),
    ]

    columns = [
        "transaction_id",
        "is_valid",
    ]

    # Create the Spark DataFrame.
    df = spark.createDataFrame(
        test_data,
        columns
    )

    # Calculate validation metrics.
    metrics = get_validation_metrics(df)

    # Verify expected counts.
    assert metrics["total_records"] == 3
    assert metrics["valid_records"] == 2
    assert metrics["invalid_records"] == 1

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