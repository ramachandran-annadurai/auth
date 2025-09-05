from pydantic import BaseModel, EmailStr
from typing import Optional

class UserRegister(BaseModel):
    username: str
    email: EmailStr
    mobile: str
    password: str
    first_name: str
    last_name: str
    user_type: str = "patient"  # "patient" or "doctor"
    
    # Patient-specific fields (optional)
    is_pregnant: Optional[bool] = None
    
    # Doctor-specific fields (optional)
    specialization: Optional[str] = None

class UserLogin(BaseModel):
    username: str
    password: str

class OTPRequest(BaseModel):
    email: EmailStr

class OTPVerify(BaseModel):
    email: EmailStr
    otp: str

class PasswordReset(BaseModel):
    email: EmailStr
    otp: str
    new_password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user_id: str        # PAT12345678 or DOC12345678
    user_type: str      # "patient" or "doctor"
    session_id: str     # Unique session identifier

class MessageResponse(BaseModel):
    message: str
    success: bool
