from fastapi import APIRouter, HTTPException, status, Header
from typing import Optional
from app.models.auth_models import (
    UserRegister, UserLogin, OTPRequest, OTPVerify, 
    PasswordReset, TokenResponse, MessageResponse
)
from app.services.auth_service import AuthService
from app.services.email_service import EmailService

router = APIRouter()
auth_service = AuthService()
email_service = EmailService()

@router.post("/register", response_model=MessageResponse)
async def register_user(user_data: UserRegister):
    result = await auth_service.register_user(user_data)
    return MessageResponse(message=result["message"], success=True)

@router.post("/send-otp", response_model=MessageResponse)
async def send_otp(request: OTPRequest):
    try:
        otp = await auth_service.generate_otp(request.email)
        await email_service.send_otp_email(request.email, otp)
        return MessageResponse(message="OTP sent successfully", success=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/verify-otp", response_model=MessageResponse)
async def verify_otp(request: OTPVerify):
    try:
        result = await auth_service.verify_otp(request.email, request.otp)
        return MessageResponse(message=result["message"], success=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/login", response_model=TokenResponse)
async def login_user(credentials: UserLogin):
    result = await auth_service.authenticate_user(credentials.username, credentials.password)
    return TokenResponse(
        access_token=result["token"],
        token_type="bearer",
        user_id=result["user_id"],
        user_type=result["user_type"],
        session_id=result["session_id"]
    )

@router.post("/forgot-password", response_model=MessageResponse)
async def forgot_password(request: OTPRequest):
    try:
        otp = await auth_service.generate_password_reset_otp(request.email)
        await email_service.send_password_reset_email(request.email, otp)
        return MessageResponse(message="Password reset OTP sent", success=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/reset-password", response_model=MessageResponse)
async def reset_password(request: PasswordReset):
    try:
        result = await auth_service.reset_password(request.email, request.otp, request.new_password)
        return MessageResponse(message=result["message"], success=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/validate-token", response_model=MessageResponse)
async def validate_token(token: str):
    try:
        result = await auth_service.validate_token(token)
        if result.get("valid"):
            return MessageResponse(message="Token is valid", success=True)
        else:
            raise HTTPException(status_code=401, detail=result.get("error", "Invalid token"))
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

@router.post("/logout", response_model=MessageResponse)
async def logout_user(authorization: Optional[str] = Header(None)):
    try:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Authorization header required")
        
        token = authorization.split(" ")[1]
        token_data = await auth_service.validate_token(token)
        
        if not token_data.get("valid"):
            raise HTTPException(status_code=401, detail="Invalid token")
        
        # Extract session_id from token
        from app.utils.security import verify_token
        payload = verify_token(token)
        session_id = payload.get("session_id")
        
        if session_id:
            result = await auth_service.logout_user(session_id)
            return MessageResponse(message=result["message"], success=True)
        else:
            return MessageResponse(message="No active session found", success=False)
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/sessions")
async def get_user_sessions(authorization: Optional[str] = Header(None)):
    try:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Authorization header required")
        
        token = authorization.split(" ")[1]
        token_data = await auth_service.validate_token(token)
        
        if not token_data.get("valid"):
            raise HTTPException(status_code=401, detail="Invalid token")
        
        user_id = token_data.get("user_id")
        sessions = await auth_service.get_user_sessions(user_id)
        
        return {"sessions": sessions, "count": len(sessions)}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/logout-all", response_model=MessageResponse)
async def logout_all_sessions(authorization: Optional[str] = Header(None)):
    try:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Authorization header required")
        
        token = authorization.split(" ")[1]
        token_data = await auth_service.validate_token(token)
        
        if not token_data.get("valid"):
            raise HTTPException(status_code=401, detail="Invalid token")
        
        user_id = token_data.get("user_id")
        result = await auth_service.logout_all_sessions(user_id)
        
        return MessageResponse(message=result["message"], success=True)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
