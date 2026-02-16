import os
from dagster import RunFailureSensorContext, run_failure_sensor

from src.utils.notifications import send_email_notification

@run_failure_sensor
def email_failure_sensor(context: RunFailureSensorContext):
    """
    Sensor that sends an email notification when a run fails.
    """
    job_name = context.dagster_run.job_name
    run_id = context.dagster_run.run_id
    
    # Get specific step failure details including stack traces
    step_failures = []
    for event in context.get_step_failure_events():
        error_info = event.event_specific_data.error.to_string() if event.event_specific_data and event.event_specific_data.error else str(event.message)
        step_failures.append(f"Step: {event.step_key}\nError:\n{error_info}")
    
    detailed_error = "\n\n".join(step_failures) if step_failures else context.failure_event.message

    subject = f"Dagster Run Failed: {job_name}"
    body = f"""
    The Dagster run for job '{job_name}' has failed.
    
    Run ID: {run_id}
    
    Error Details:
    {detailed_error}
    
    Please check the Dagster UI for more details.
    """
    
    sender = os.getenv("SES_SENDER_EMAIL")
    recipient = os.getenv("SES_RECIPIENT_EMAIL")
    aws_region = os.getenv("AWS_REGION", "us-west-1")
    
    if sender and recipient:
        send_email_notification(
            subject=subject,
            body_text=body,
            sender=sender,
            recipient=recipient,
            aws_region=aws_region,
        )
    else:
        context.log.warning("SES_SENDER_EMAIL or SES_RECIPIENT_EMAIL not set. Skipping email notification.")
