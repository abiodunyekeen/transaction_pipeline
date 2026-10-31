
# ---------------------------------------------------------
# SHARED BUSINESS CONFIGURATION
# ---------------------------------------------------------
#
# These settings are business rules that are the same
# regardless of where the pipeline runs.

COMMON_CONFIG = {
    "pipeline_name": "transaction_pipeline",

    "allowed_countries": [
        "UK",
        "US",
        "NG",
        "DE",
    ],
}


# ---------------------------------------------------------
# ENVIRONMENT DEFINITIONS
# ---------------------------------------------------------
#
# Each environment uses its own Databricks schema.
#
# The TABLE NAMES stay the same.
# The SCHEMA changes.
#
# Example:
#
# DEV:
# workspace.dev.silver_transactions
#
# PROD:
# workspace.prod.silver_transactions

ENVIRONMENTS = {

    "DEV": {
        "catalog": "workspace",
        "schema": "dev",
    },

    "TEST": {
        "catalog": "workspace",
        "schema": "test",
    },

    "STAGING": {
        "catalog": "workspace",
        "schema": "staging",
    },

    "PROD": {
        "catalog": "workspace",
        "schema": "prod",
    },
}


def get_config(environment: str) -> dict:
    """
    Build the complete configuration for one environment.

    Parameters
    ----------
    environment:
        DEV, TEST, STAGING or PROD.

    Returns
    -------
    dict
        Environment-specific pipeline configuration.
    """

    # Make environment handling case-insensitive.
    environment = environment.upper()

    # Fail immediately if somebody provides an unknown
    # environment rather than silently defaulting somewhere.
    if environment not in ENVIRONMENTS:
        raise ValueError(
            f"Unsupported PIPELINE_ENV: {environment}. "
            f"Expected one of {list(ENVIRONMENTS.keys())}"
        )

    environment_config = ENVIRONMENTS[environment]

    catalog = environment_config["catalog"]
    schema = environment_config["schema"]

    # This produces names such as:
    #
    # workspace.dev.bronze_transactions
    # workspace.test.silver_transactions
    # workspace.prod.gold_daily_country

    return {
        **COMMON_CONFIG,

        "environment": environment,

        "catalog": catalog,
        "schema": schema,

        "source_table":
            f"{catalog}.{schema}.bronze_transactions",

        "silver_table":
            f"{catalog}.{schema}.silver_transactions",

        "gold_table":
            f"{catalog}.{schema}.gold_daily_country",

        "quarantine_table":
            f"{catalog}.{schema}.quarantine_transactions",

        "control_table":
            f"{catalog}.{schema}.pipeline_batch_control",
    }