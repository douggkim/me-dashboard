"""Tests for the email failure sensor."""

import os
from unittest import mock

from dagster import DagsterEvent, DagsterEventType, DagsterInstance, DagsterRun, build_run_status_sensor_context

from src.sensors.email_failure_sensor import email_failure_sensor


class TestEmailFailureSensor:
    """Tests for the email failure sensor."""

    @mock.patch("src.sensors.email_failure_sensor.send_email_notification")
    def test_email_failure_sensor_triggers_email(self, mock_send_email: mock.MagicMock) -> None:
        """Test that the sensor calls send_email_notification on failure."""
        # Mock environment variables
        with mock.patch.dict(
            os.environ,
            {
                "SES_SENDER_EMAIL": "sender@example.com",
                "SES_RECIPIENT_EMAIL": "recipient@example.com",
                "AWS_REGION": "us-west-2",
            },
        ):
            instance = DagsterInstance.ephemeral()

            # Create real DagsterRun and DagsterEvent objects
            dagster_run = DagsterRun(job_name="test_job", run_id="test-run-id")

            # We need a DagsterEvent of type RUN_FAILURE
            # Constructing a minimal DagsterEvent for failure
            dagster_event = DagsterEvent(
                event_type_value=DagsterEventType.RUN_FAILURE.value,
                job_name="test_job",
                message="Something went wrong!",
            )

            # Use build_run_status_sensor_context and convert to failure context
            context = build_run_status_sensor_context(
                sensor_name="email_failure_sensor",
                dagster_instance=instance,
                dagster_run=dagster_run,
                dagster_event=dagster_event,
            ).for_run_failure()

            # Call the sensor function directly
            email_failure_sensor(context)

            # Verify email was sent
            mock_send_email.assert_called_once()
            call_args = mock_send_email.call_args[1]

            assert "Dagster Run Failed: test_job" in call_args["subject"]
            assert "test-run-id" in call_args["body_text"]
            assert "Something went wrong!" in call_args["body_text"]
            assert call_args["sender"] == "sender@example.com"
            assert call_args["recipient"] == "recipient@example.com"
            assert call_args["aws_region"] == "us-west-2"

    @mock.patch("src.sensors.email_failure_sensor.send_email_notification")
    def test_email_failure_sensor_with_step_failure(self, mock_send_email: mock.MagicMock) -> None:
        """Test that the sensor includes detailed step failure information."""
        with mock.patch.dict(
            os.environ, {"SES_SENDER_EMAIL": "sender@example.com", "SES_RECIPIENT_EMAIL": "recipient@example.com"}
        ):
            instance = DagsterInstance.ephemeral()
            dagster_run = DagsterRun(job_name="test_job", run_id="test-run-id")
            dagster_event = DagsterEvent(
                event_type_value=DagsterEventType.RUN_FAILURE.value, job_name="test_job", message="Run failed"
            )

            context = build_run_status_sensor_context(
                sensor_name="email_failure_sensor",
                dagster_instance=instance,
                dagster_run=dagster_run,
                dagster_event=dagster_event,
            ).for_run_failure()

            # Mock get_step_failure_events to return a fake event with error details
            mock_step_event = mock.Mock()
            mock_step_event.step_key = "my_step"
            mock_step_event.event_specific_data.error.to_string.return_value = "Detailed Stack Trace\nError: ValueError"

            # Patch the method on the class itself
            with mock.patch(
                "dagster._core.definitions.run_status_sensor_definition.RunFailureSensorContext.get_step_failure_events",
                return_value=[mock_step_event],
            ):
                email_failure_sensor(context)

            mock_send_email.assert_called_once()
            call_args = mock_send_email.call_args[1]

            # Verify body contains the detailed error
            assert "Step: my_step" in call_args["body_text"]
            assert "Detailed Stack Trace" in call_args["body_text"]
            assert "Error: ValueError" in call_args["body_text"]

    @mock.patch("src.sensors.email_failure_sensor.send_email_notification")
    def test_email_failure_sensor_skips_if_env_vars_missing(self, mock_send_email: mock.MagicMock) -> None:
        """Test that the sensor skips sending email if env vars are missing."""
        # Mock environment variables to be empty
        with mock.patch.dict(os.environ, {}, clear=True):
            # Create a failure context
            instance = DagsterInstance.ephemeral()
            dagster_run = DagsterRun(job_name="test_job", run_id="test-run-id")
            dagster_event = DagsterEvent(
                event_type_value=DagsterEventType.RUN_FAILURE.value,
                job_name="test_job",
                message="Something went wrong!",
            )

            context = build_run_status_sensor_context(
                sensor_name="email_failure_sensor",
                dagster_instance=instance,
                dagster_run=dagster_run,
                dagster_event=dagster_event,
            ).for_run_failure()

            # Call the sensor function directly
            email_failure_sensor(context)

            # Verify email was NOT sent
            mock_send_email.assert_not_called()
