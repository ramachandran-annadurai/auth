"""
Custom exception classes for better error handling
"""
from typing import Optional, Dict, Any
from fastapi import HTTPException, status

class AuthServiceException(Exception):
    """Base exception for auth service"""
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        self.message = message
        self.error_code = error_code or "AUTH_ERROR"
        self.details = details or {}
        super().__init__(self.message)

class ValidationError(AuthServiceException):
    """Input validation errors"""
    def __init__(self, message: str, field: str = None, details: Dict[str, Any] = None):
        super().__init__(message, "VALIDATION_ERROR", details)
        self.field = field

class AuthenticationError(AuthServiceException):
    """Authentication failures"""
    def __init__(self, message: str = "Authentication failed", details: Dict[str, Any] = None):
        super().__init__(message, "AUTH_FAILED", details)

class AuthorizationError(AuthServiceException):
    """Authorization failures"""
    def __init__(self, message: str = "Access denied", details: Dict[str, Any] = None):
        super().__init__(message, "ACCESS_DENIED", details)

class UserExistsError(AuthServiceException):
    """User already exists"""
    def __init__(self, message: str, user_id: str = None, user_type: str = None):
        details = {}
        if user_id:
            details["existing_user_id"] = user_id
        if user_type:
            details["user_type"] = user_type
        super().__init__(message, "USER_EXISTS", details)

class UserNotFoundError(AuthServiceException):
    """User not found"""
    def __init__(self, message: str = "User not found", identifier: str = None):
        details = {}
        if identifier:
            details["identifier"] = identifier
        super().__init__(message, "USER_NOT_FOUND", details)

class OTPError(AuthServiceException):
    """OTP related errors"""
    def __init__(self, message: str, otp_type: str = None):
        details = {}
        if otp_type:
            details["otp_type"] = otp_type
        super().__init__(message, "OTP_ERROR", details)

class SessionError(AuthServiceException):
    """Session related errors"""
    def __init__(self, message: str, session_id: str = None):
        details = {}
        if session_id:
            details["session_id"] = session_id
        super().__init__(message, "SESSION_ERROR", details)

class DatabaseError(AuthServiceException):
    """Database operation errors"""
    def __init__(self, message: str = "Database operation failed", operation: str = None):
        details = {}
        if operation:
            details["operation"] = operation
        super().__init__(message, "DATABASE_ERROR", details)

class EmailError(AuthServiceException):
    """Email sending errors"""
    def __init__(self, message: str = "Email sending failed", email: str = None):
        details = {}
        if email:
            details["email"] = email
        super().__init__(message, "EMAIL_ERROR", details)

class RateLimitError(AuthServiceException):
    """Rate limiting errors"""
    def __init__(self, message: str = "Rate limit exceeded", retry_after: int = None):
        details = {}
        if retry_after:
            details["retry_after"] = retry_after
        super().__init__(message, "RATE_LIMIT", details)

class ConfigurationError(AuthServiceException):
    """Configuration errors"""
    def __init__(self, message: str, config_key: str = None):
        details = {}
        if config_key:
            details["config_key"] = config_key
        super().__init__(message, "CONFIG_ERROR", details)

# HTTP Exception mappings
EXCEPTION_STATUS_MAP = {
    ValidationError: status.HTTP_422_UNPROCESSABLE_ENTITY,
    AuthenticationError: status.HTTP_401_UNAUTHORIZED,
    AuthorizationError: status.HTTP_403_FORBIDDEN,
    UserExistsError: status.HTTP_409_CONFLICT,
    UserNotFoundError: status.HTTP_404_NOT_FOUND,
    OTPError: status.HTTP_400_BAD_REQUEST,
    SessionError: status.HTTP_401_UNAUTHORIZED,
    DatabaseError: status.HTTP_500_INTERNAL_SERVER_ERROR,
    EmailError: status.HTTP_503_SERVICE_UNAVAILABLE,
    RateLimitError: status.HTTP_429_TOO_MANY_REQUESTS,
    ConfigurationError: status.HTTP_500_INTERNAL_SERVER_ERROR,
}

def auth_exception_to_http(exception: AuthServiceException) -> HTTPException:
    """Convert auth service exception to HTTP exception"""
    status_code = EXCEPTION_STATUS_MAP.get(type(exception), status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    detail = {
        "error_code": exception.error_code,
        "message": exception.message,
        "details": exception.details
    }
    
    return HTTPException(status_code=status_code, detail=detail)
