import pytest

# Import datetime so our test rows contain proper timestamps.
from datetime import datetime


# Import the function being tested.
from src.silver import deduplicate_latest_transactions

@pytest.mark.spark
def test_latest_transaction_version_is_retained(spark):
    """
    When multiple versions of the same transaction exist,
    the row with the newest updated_at should survive.
    """

    # Create two versions of TX100.
    #
    # The second row is newer and should therefore survive.
    test_data = [
        (
            "TX100",
            1,
            100.0,
            datetime(2026, 9, 20, 10, 0, 0)
        ),
        (
            "TX100",
            1,
            130.0,
            datetime(2026, 9, 20, 11, 0, 0)
        ),
    ]

    columns = [
        "transaction_id",
        "quantity",
        "total_amount",
        "updated_at",
    ]

    # Create test DataFrame.
    df = spark.createDataFrame(
        test_data,
        columns
    )

    # Deduplicate using latest-version logic.
    result_df = deduplicate_latest_transactions(df)


    # There should now be exactly one TX100 row.
    result = result_df.first()

    # Confirm the newer version survived.
    assert result["total_amount"] == 130.0

    # Confirm only one logical transaction remains.
    assert result_df.count() == 1