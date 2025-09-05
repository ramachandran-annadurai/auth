import pytest
from unittest.mock import Mock, patch, MagicMock
from app.services.email_service import EmailService

class TestEmailService:
    
    @pytest.fixture
    def email_service(self):
        """Create EmailService instance with mocked settings"""
        with patch('app.services.email_service.settings') as mock_settings:
            mock_settings.smtp_host = "smtp.gmail.com"
            mock_settings.smtp_port = 587
            mock_settings.smtp_user = "test@gmail.com"
            mock_settings.smtp_password = "test_password"
            return EmailService()
    
    @pytest.mark.asyncio
    async def test_send_otp_email_success(self, email_service):
        """Test successful OTP email sending"""
        with patch('smtplib.SMTP') as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server
            
            await email_service.send_otp_email("patient@test.com", "123456")
            
            # Verify SMTP connection
            mock_smtp.assert_called_once_with("smtp.gmail.com", 587)
            mock_server.starttls.assert_called_once()
            mock_server.login.assert_called_once_with("test@gmail.com", "test_password")
            mock_server.send_message.assert_called_once()
            
            # Verify email content
            sent_message = mock_server.send_message.call_args[0][0]
            assert sent_message['To'] == "patient@test.com"
            assert sent_message['From'] == "test@gmail.com"
            assert sent_message['Subject'] == "Patient Portal - Email Verification"
            assert "123456" in sent_message.get_payload()
    
    @pytest.mark.asyncio
    async def test_send_password_reset_email_success(self, email_service):
        """Test successful password reset email sending"""
        with patch('smtplib.SMTP') as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server
            
            await email_service.send_password_reset_email("doctor@test.com", "654321")
            
            # Verify email content
            sent_message = mock_server.send_message.call_args[0][0]
            assert sent_message['To'] == "doctor@test.com"
            assert sent_message['Subject'] == "Patient Portal - Password Reset"
            assert "654321" in sent_message.get_payload()
    
    @pytest.mark.asyncio
    async def test_send_otp_email_smtp_error(self, email_service):
        """Test OTP email sending with SMTP error"""
        with patch('smtplib.SMTP') as mock_smtp:
            mock_smtp.side_effect = Exception("SMTP connection failed")
            
            with pytest.raises(Exception) as exc_info:
                await email_service.send_otp_email("test@test.com", "123456")
            
            assert "Failed to send email" in str(exc_info.value)
            assert "SMTP connection failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_send_password_reset_email_smtp_error(self, email_service):
        """Test password reset email sending with SMTP error"""
        with patch('smtplib.SMTP') as mock_smtp:
            mock_smtp.side_effect = Exception("Authentication failed")
            
            with pytest.raises(Exception) as exc_info:
                await email_service.send_password_reset_email("test@test.com", "123456")
            
            assert "Failed to send email" in str(exc_info.value)
            assert "Authentication failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_email_content_format(self, email_service):
        """Test email content formatting"""
        with patch('smtplib.SMTP') as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server
            
            test_email = "patient@hospital.com"
            test_otp = "987654"
            
            await email_service.send_otp_email(test_email, test_otp)
            
            sent_message = mock_server.send_message.call_args[0][0]
            email_body = sent_message.get_payload()
            
            # Verify OTP is in the email body
            assert test_otp in email_body
            assert "Dear Patient" in email_body
            assert "verification code" in email_body
            assert "10 minutes" in email_body
            assert "Patient Portal Team" in email_body
