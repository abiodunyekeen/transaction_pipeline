# Create configuration for the transaction pipeline.
# Table names and other settings store here instead of
# hard-coding them throughout the transaction code.

CONFIG = {
    #Source / Bronze table
    "bronze_table": "bronze_transactions",
    # Cleaned Silver table
    "silver_table": "silver_transactions",
    # Aggregation Gold table
    "gold_table": "gold_daily_country",
    # Control table used to track pipeline batches.
    "control_table": "pipeline_batch_control",
    # Name used in logs and control-table records.
    "pipeline_name": "transaction_pipeline",

    "checkpoint_path":
        "/Volumes/workspace/default/checkpoints/transactions",
    # Countries allowed by our current business rule
    "allowed_countries":
        ["UK", "US", "NG", "DE"]
}