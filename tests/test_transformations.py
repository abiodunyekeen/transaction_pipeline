import pytest
# Import the transformation to test
from src.transformations import calculate_transaction_amount

@pytest.mark.spark
def test_transaction_amount_is_calculated_correctly(spark):
    """
    Verify that:
        amount = quantity * unit_price
    """

    # Create a small deterministic test dataset.
    test_data = [
        (
            "TX001",
            2,
            25.50
        )
    ]

    columns = [
        "transaction_id",
        "quantity",
        "unit_price"
    ]

    # Convert the Python test data into a Spark DataFrame.
    df = spark.createDataFrame(
        test_data,
        columns
    )

    # Apply the transformation.
    result_df = calculate_transaction_amount(df)

    # Retrieve the single test row.
    result = result_df.first()

    # 2 × 25.50 should equal 51.00.
    assert result["total_amount"] == 51.0

@pytest.mark.spark
def test_transaction_amount_with_multiple_rows(spark):
    """
    Verify amount calculation for several transactions.
    """

    # Create several known inputs.
    test_data = [
        ("TX001", 2, 25.0),
        ("TX002", 3, 10.0),
        ("TX003", 1, 100.0),
    ]

    columns = [
        "transaction_id",
        "quantity",
        "unit_price"
    ]

    # Create the Spark DataFrame.
    df = spark.createDataFrame(
        test_data,
        columns
    )

    # Calculate transaction amounts.
    result_df = calculate_transaction_amount(df)

    # Order the result to make the test deterministic.
    results = (
        result_df
        .orderBy("transaction_id")
        .collect()
    )

    # Verify each expected amount.
    assert results[0]["total_amount"] == 50.0
    assert results[1]["total_amount"] == 30.0
    assert results[2]["total_amount"] == 100.0