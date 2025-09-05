import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.utils.config import settings

class EmailService:
    def __init__(self):
        self.smtp_host = settings.smtp_host
        self.smtp_port = settings.smtp_port
        self.smtp_user = settings.smtp_user
        self.smtp_password = settings.smtp_password

    async def send_otp_email(self, email: str, otp: str):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_user
            msg['To'] = email
            msg['Subject'] = "Patient Portal - Email Verification"

            body = f"""
            Dear Patient,

            Your verification code is: {otp}

            This code will expire in 10 minutes.
            Please do not share this code with anyone.

            Best regards,
            Patient Portal Team
            """

            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
                
        except Exception as e:
            raise Exception(f"Failed to send email: {str(e)}")

    async def send_password_reset_email(self, email: str, otp: str):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_user
            msg['To'] = email
            msg['Subject'] = "Patient Portal - Password Reset"

            body = f"""
            Dear Patient,

            Your password reset code is: {otp}

            This code will expire in 10 minutes.
            If you did not request this password reset, please ignore this email.

            Best regards,
            Patient Portal Team
            """

            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
                
        except Exception as e:
            raise Exception(f"Failed to send email: {str(e)}")
