import pytest

# Import date for transaction_date values.
from datetime import date

# Import the Gold transformation being tested.
from src.gold import build_daily_country_gold

@pytest.mark.spark
def test_daily_country_gold_aggregation(spark):
    """
    Verify that Gold correctly aggregates
    transactions by transaction_date and country.
    """

    # Create a tiny deterministic Silver dataset.
    test_data = [
        # Same date, same country, same customer.
        (
            "TX001",
            "C100",
            date(2026, 9, 20),
            "UK",
            20.0
        ),
        (
            "TX002",
            "C100",
            date(2026, 9, 20),
            "UK",
            30.0
        ),

        # Same date/country but different customer.
        (
            "TX003",
            "C200",
            date(2026, 9, 20),
            "UK",
            50.0
        ),

        # Different country.
        (
            "TX004",
            "C300",
            date(2026, 9, 20),
            "US",
            40.0
        )
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "transaction_date",
        "country",
        "total_amount"
    ]

    # Create the Silver-style input DataFrame.
    silver_df = spark.createDataFrame(
        test_data,
        columns
    )

    # Build the Gold aggregate.
    gold_df = build_daily_country_gold(
        silver_df
    )

    # Select the UK group we want to verify.
    uk_result = (
        gold_df
        .filter(
            gold_df.country == "UK"
        )
        .first()
    )

    # UK revenue:
    # 20 + 30 + 50 = 100
    assert uk_result["total_revenue"] == 100.0

    # Three UK transactions.
    assert uk_result["transaction_count"] == 3

    # C100 and C200 = two unique customers.
    assert uk_result["unique_customer"] == 2

    # Average:
    # 100 / 3 = approximately 33.3333
    assert round(
        uk_result["average_transaction_value"],
        2
    ) == 33.33