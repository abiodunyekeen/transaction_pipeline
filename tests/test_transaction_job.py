# Import pytest so we can verify that an exception
# is correctly propagated by the pipeline.
import pytest

# Import patch and MagicMock for replacing real
# external dependencies with controlled test objects.
from unittest.mock import patch, MagicMock

# Import the job module that we want to test.
from jobs import transaction_job


def test_pipeline_failure_does_not_mark_batch_success():
    """
    If the pipeline fails, main() should:

    - mark the batch as FAILED,
    - NOT mark it as SUCCESS,
    - re-raise the original exception.

    This protects us from silently recording a failed
    pipeline as successful.
    """

    # Create a fake Spark object.
    #
    # No real Databricks connection is needed for this test.
    fake_spark = MagicMock()

    # Patch functions where transaction_job.py uses them.
    #
    # This is important:
    # we patch jobs.transaction_job.<function>,
    # not necessarily the module where the function
    # was originally defined.
    with (
        patch(
            "jobs.transaction_job.get_spark_session",
            return_value=fake_spark
        ),

        patch(
            "jobs.transaction_job.create_control_table"
        ) as mock_create_control,

        patch(
            "jobs.transaction_job.mark_batch_started"
        ) as mock_started,

        patch(
            "jobs.transaction_job.run_pipeline",
            side_effect=RuntimeError(
                "Simulated Silver failure"
            )
        ) as mock_run_pipeline,

        patch(
            "jobs.transaction_job.mark_batch_success"
        ) as mock_success,

        patch(
            "jobs.transaction_job.mark_batch_failed"
        ) as mock_failed
    ):

        # main() should re-raise the RuntimeError.
        #
        # If main() swallows the exception, this test fails.
        with pytest.raises(
            RuntimeError,
            match="Simulated Silver failure"
        ):
            transaction_job.main()

        # Verify the control table was prepared.
        mock_create_control.assert_called_once()

        # Verify the batch was initially marked PROCESSING.
        mock_started.assert_called_once()

        # Verify the actual pipeline was attempted.
        mock_run_pipeline.assert_called_once()

        # Critical assertion:
        # a failed pipeline must NEVER be marked SUCCESS.
        mock_success.assert_not_called()

        # The failed batch should be recorded.
        mock_failed.assert_called_once()


def test_successful_pipeline_marks_batch_success():
    """
    When the complete pipeline succeeds:

    - the batch should be marked SUCCESS,
    - it should not be marked FAILED.
    """

    # Create a fake Spark object.
    fake_spark = MagicMock()

    with (
        patch(
            "jobs.transaction_job.get_spark_session",
            return_value=fake_spark
        ),

        patch(
            "jobs.transaction_job.create_control_table"
        ),

        patch(
            "jobs.transaction_job.mark_batch_started"
        ) as mock_started,

        # This mock completes successfully because
        # no side_effect exception is configured.
        patch(
            "jobs.transaction_job.run_pipeline"
        ) as mock_run_pipeline,

        patch(
            "jobs.transaction_job.mark_batch_success"
        ) as mock_success,

        patch(
            "jobs.transaction_job.mark_batch_failed"
        ) as mock_failed
    ):

        # Run the job successfully.
        transaction_job.main()

        # The batch should have been started.
        mock_started.assert_called_once()

        # Pipeline processing should have occurred.
        mock_run_pipeline.assert_called_once()

        # Successful processing must mark SUCCESS.
        mock_success.assert_called_once()

        # No failure record should be written.
        mock_failed.assert_not_called()