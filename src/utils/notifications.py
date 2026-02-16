import os
import boto3
from botocore.exceptions import ClientError
from loguru import logger

def send_email_notification(
    subject: str,
    body_text: str,
    sender: str,
    recipient: str,
    aws_region: str = "us-west-1",
):
    """Send an email notification using Amazon SES.
    
    If DAGSTER_ENV is set to 'local', the email content is logged instead of sent.

    Args:
        subject (str): The subject line for the email.
        body_text (str): The body text of the email.
        sender (str): The email address that is sending the email.
        recipient (str): The email address to receive the email.
        aws_region (str): The AWS region to use.

    Raises:
        ClientError: If the email could not be sent.
    """
    if os.environ.get("DAGSTER_ENV") == "local":
        logger.info(f"Email simulated: To: {recipient}, Subject: {subject}, Body: {body_text}")
        return

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=aws_region)

    try:
        logger.info(f"Attempting to send email via SES to {recipient} with subject '{subject}' in region {aws_region}")
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    recipient,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': "UTF-8",
                        'Data': body_text,
                    },
                },
                'Subject': {
                    'Charset': "UTF-8",
                    'Data': subject,
                },
            },
            Source=sender,
        )
    except ClientError as e:
        logger.error(f"Failed to send email to {recipient}: {e.response['Error']['Message']}")
        raise e
    else:
        logger.info(f"Email sent successfully to {recipient}! Message ID: {response['MessageId']}")

if __name__ == "__main__":
    send_email_notification(
        "Test Subject",
        "This is a test email body.",
        "sender@example.com",
        "recipient@example.com",
    )
