"""
Error handling middleware and utilities
"""
import logging
import traceback
from datetime import datetime
from typing import Dict, Any
from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.utils.exceptions import AuthServiceException, auth_exception_to_http

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ErrorHandler:
    """Centralized error handling"""
    
    @staticmethod
    def log_error(error: Exception, request: Request = None, user_id: str = None):
        """Log error with context"""
        context = {
            "timestamp": datetime.utcnow().isoformat(),
            "error_type": type(error).__name__,
            "error_message": str(error),
        }
        
        if request:
            context.update({
                "method": request.method,
                "url": str(request.url),
                "client_ip": request.client.host if request.client else None,
                "user_agent": request.headers.get("user-agent"),
            })
        
        if user_id:
            context["user_id"] = user_id
            
        if isinstance(error, AuthServiceException):
            context.update({
                "error_code": error.error_code,
                "error_details": error.details
            })
            logger.warning(f"Auth Service Error: {context}")
        else:
            context["traceback"] = traceback.format_exc()
            logger.error(f"Unexpected Error: {context}")
    
    @staticmethod
    def create_error_response(
        error_code: str,
        message: str,
        status_code: int = 500,
        details: Dict[str, Any] = None
    ) -> JSONResponse:
        """Create standardized error response"""
        content = {
            "success": False,
            "error": {
                "code": error_code,
                "message": message,
                "timestamp": datetime.utcnow().isoformat(),
                "details": details or {}
            }
        }
        
        return JSONResponse(
            status_code=status_code,
            content=content
        )

# Global exception handlers
async def auth_service_exception_handler(request: Request, exc: AuthServiceException):
    """Handle custom auth service exceptions"""
    ErrorHandler.log_error(exc, request)
    
    http_exc = auth_exception_to_http(exc)
    return ErrorHandler.create_error_response(
        error_code=exc.error_code,
        message=exc.message,
        status_code=http_exc.status_code,
        details=exc.details
    )

async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle Pydantic validation errors"""
    ErrorHandler.log_error(exc, request)
    
    # Extract validation error details
    error_details = []
    for error in exc.errors():
        error_details.append({
            "field": ".".join(str(x) for x in error["loc"]),
            "message": error["msg"],
            "type": error["type"]
        })
    
    return ErrorHandler.create_error_response(
        error_code="VALIDATION_ERROR",
        message="Input validation failed",
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        details={"validation_errors": error_details}
    )

async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle FastAPI HTTP exceptions"""
    ErrorHandler.log_error(exc, request)
    
    return ErrorHandler.create_error_response(
        error_code="HTTP_ERROR",
        message=exc.detail if isinstance(exc.detail, str) else "HTTP error occurred",
        status_code=exc.status_code,
        details=exc.detail if isinstance(exc.detail, dict) else {}
    )

async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions"""
    ErrorHandler.log_error(exc, request)
    
    # Don't expose internal errors in production
    return ErrorHandler.create_error_response(
        error_code="INTERNAL_ERROR",
        message="An internal server error occurred",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"type": type(exc).__name__} if not is_production() else {}
    )

def is_production() -> bool:
    """Check if running in production environment"""
    import os
    return os.getenv("ENVIRONMENT", "development").lower() == "production"

# Rate limiting utilities
class RateLimiter:
    """Simple in-memory rate limiter"""
    
    def __init__(self):
        self.attempts = {}  # {key: [(timestamp, count), ...]}
        
    def is_rate_limited(self, key: str, max_attempts: int = 5, window_minutes: int = 15) -> bool:
        """Check if key is rate limited"""
        import time
        
        now = time.time()
        window_start = now - (window_minutes * 60)
        
        # Clean old attempts
        if key in self.attempts:
            self.attempts[key] = [
                (timestamp, count) for timestamp, count in self.attempts[key]
                if timestamp > window_start
            ]
        
        # Count current attempts
        current_attempts = sum(
            count for timestamp, count in self.attempts.get(key, [])
            if timestamp > window_start
        )
        
        return current_attempts >= max_attempts
    
    def record_attempt(self, key: str):
        """Record an attempt"""
        import time
        
        now = time.time()
        if key not in self.attempts:
            self.attempts[key] = []
        
        self.attempts[key].append((now, 1))

# Global rate limiter instance
rate_limiter = RateLimiter()
