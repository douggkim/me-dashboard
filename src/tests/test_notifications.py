import os
from unittest import mock
import pytest
from src.utils.notifications import send_email_notification

class TestEmailNotification:
    
    @mock.patch("boto3.client")
    def test_send_email_notification_success(self, mock_boto_client):
        """Test that send_email_notification calls boto3 correctly."""
        # Setup mock
        mock_ses = mock.Mock()
        mock_boto_client.return_value = mock_ses
        
        mock_ses.send_email.return_value = {"MessageId": "test-message-id"}
        
        # Call function
        send_email_notification(
            subject="Test Subject",
            body_text="Test Body",
            sender="sender@example.com",
            recipient="recipient@example.com"
        )
        
        # Verify boto3 call
        mock_ses.send_email.assert_called_once()
        call_args = mock_ses.send_email.call_args[1]
        
        assert call_args["Source"] == "sender@example.com"
        assert call_args["Destination"]["ToAddresses"] == ["recipient@example.com"]
        assert call_args["Message"]["Subject"]["Data"] == "Test Subject"
        assert call_args["Message"]["Body"]["Text"]["Data"] == "Test Body"

    @mock.patch("src.utils.notifications.logger")
    def test_send_email_notification_local_env(self, mock_logger):
        """Test that email is mocked (logged) when DAGSTER_ENV is local."""
        with mock.patch.dict(os.environ, {"DAGSTER_ENV": "local"}):
             send_email_notification(
                subject="Test Subject",
                body_text="Test Body",
                sender="sender@example.com",
                recipient="recipient@example.com"
            )
             
        # Verify logger was called and no boto3 client was created (implicitly, as it would fail without mock if attempted)
        mock_logger.info.assert_called_once()
        assert "Email simulated" in mock_logger.info.call_args[0][0]
