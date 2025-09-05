from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class UserSession(BaseModel):
    session_id: str
    user_id: str
    user_type: str
    device_info: Optional[str] = None
    ip_address: Optional[str] = None
    login_time: datetime
    last_activity: datetime
    expires_at: datetime
    is_active: bool = True

class SessionResponse(BaseModel):
    session_id: str
    user_id: str
    user_type: str
    login_time: str
    expires_at: str
    is_active: bool
