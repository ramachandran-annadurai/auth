import bcrypt
import random
import string
import uuid
from datetime import datetime, timedelta
from app.database.mongo_client import get_database
from app.utils.security import create_access_token, verify_password, hash_password
from app.models.auth_models import UserRegister
from app.utils.config import settings
from app.utils.exceptions import (
    ValidationError, AuthenticationError, UserExistsError, 
    UserNotFoundError, OTPError, SessionError, DatabaseError
)

class AuthService:
    def __init__(self):
        self.db = get_database()
        self.patients_collection = self.db[settings.patients_collection_name]
        self.doctors_collection = self.db[settings.doctors_collection_name]
        self.pending_users_collection = self.db[settings.pending_users_collection_name]
        self.otp_collection = self.db[settings.otp_codes_collection_name]
        self.sessions_collection = self.db[settings.user_sessions_collection_name]

    async def register_user(self, user_data: UserRegister):
        # Validate user type
        if user_data.user_type not in ["patient", "doctor"]:
            raise ValidationError(
                "Invalid user type. Must be 'patient' or 'doctor'",
                field="user_type",
                details={"allowed_values": ["patient", "doctor"]}
            )
            
        # Check if user exists (check all collections including pending)
        existing_patient = self.patients_collection.find_one({
            "$or": [
                {"email": user_data.email},
                {"username": user_data.username}
            ]
        })
        
        existing_doctor = self.doctors_collection.find_one({
            "$or": [
                {"email": user_data.email},
                {"username": user_data.username}
            ]
        })
        
        existing_pending = self.pending_users_collection.find_one({
            "$or": [
                {"email": user_data.email},
                {"username": user_data.username}
            ]
        })
        
        # Provide specific error messages
        if existing_patient:
            raise UserExistsError(
                f"A verified patient account already exists with this email/username",
                user_id=existing_patient['user_id'],
                user_type="patient"
            )
        
        if existing_doctor:
            raise UserExistsError(
                f"A verified doctor account already exists with this email/username",
                user_id=existing_doctor['user_id'],
                user_type="doctor"
            )
        
        if existing_pending:
            # Check if pending registration has expired
            if existing_pending["expires_at"] < datetime.utcnow():
                # Remove expired pending registration
                self.pending_users_collection.delete_one({"_id": existing_pending["_id"]})
            else:
                remaining_time = existing_pending["expires_at"] - datetime.utcnow()
                remaining_minutes = remaining_time.total_seconds() / 60
                raise UserExistsError(
                    f"Registration is pending for this email/username. Please verify OTP or wait {remaining_minutes:.1f} minutes for expiration.",
                    user_id=existing_pending['user_id'],
                    user_type=existing_pending['user_type']
                )
        


        # Generate user ID based on type
        user_id = self._generate_user_id(user_data.user_type)
        
        # Hash password
        hashed_password = hash_password(user_data.password)
        
        # Create pending user document (not the actual account yet)
        pending_user_doc = {
            "user_id": user_id,
            "username": user_data.username,
            "email": user_data.email,
            "mobile": user_data.mobile,
            "password_hash": hashed_password,
            "first_name": user_data.first_name,
            "last_name": user_data.last_name,
            "user_type": user_data.user_type,
            "created_at": datetime.utcnow(),
            "expires_at": datetime.utcnow() + timedelta(minutes=30)  # Pending registration expires in 30 minutes
        }
        
        # Add type-specific fields
        if user_data.user_type == "patient":
            pending_user_doc["is_pregnant"] = user_data.is_pregnant or False
        elif user_data.user_type == "doctor":
            pending_user_doc["specialization"] = user_data.specialization
        
        # Store in pending_users collection (not the actual users collection yet)
        self.pending_users_collection.insert_one(pending_user_doc)
        
        # Generate and send OTP automatically
        otp = await self.generate_otp(user_data.email)
        
        return {
            "message": "Registration initiated. Please verify OTP to complete account creation.", 
            "user_id": user_id,
            "status": "pending_verification",
            "expires_in_minutes": 30
        }

    async def authenticate_user(self, username: str, password: str):
        # Find user by username, email, or user_id (search both collections)
        user = None
        
        # First try patients collection
        user = self.patients_collection.find_one({
            "$or": [
                {"username": username},
                {"email": username},
                {"user_id": username}  # PAT12345678
            ]
        })
        
        # If not found in patients, try doctors collection
        if not user:
            user = self.doctors_collection.find_one({
                "$or": [
                    {"username": username},
                    {"email": username},
                    {"user_id": username}  # DOC12345678
                ]
            })
        
        if not user:
            raise AuthenticationError("Invalid credentials")
        
        if not user.get("is_verified", False):
            raise AuthenticationError("Account not verified. Please verify OTP first.")
        
        if not verify_password(password, user["password_hash"]):
            raise AuthenticationError("Invalid credentials")
        
        # Create user session
        session_id = str(uuid.uuid4())
        session_doc = {
            "session_id": session_id,
            "user_id": user["user_id"],
            "user_type": user["user_type"],
            "login_time": datetime.utcnow(),
            "last_activity": datetime.utcnow(),
            "expires_at": datetime.utcnow() + timedelta(minutes=30),
            "is_active": True
        }
        self.sessions_collection.insert_one(session_doc)
        
        # Create access token with session info
        token_data = {
            "sub": user["user_id"],
            "user_type": user["user_type"],
            "session_id": session_id
        }
        token = create_access_token(token_data)
        
        return {
            "token": token,
            "user_id": user["user_id"],
            "user_type": user["user_type"],
            "session_id": session_id
        }

    async def generate_otp(self, email: str):
        # Check if user exists (check verified users first, then pending users)
        user = self.patients_collection.find_one({"email": email})
        if not user:
            user = self.doctors_collection.find_one({"email": email})
        
        # If not found in verified users, check pending users
        if not user:
            user = self.pending_users_collection.find_one({"email": email})
        
        if not user:
            raise UserNotFoundError("User not found", identifier=email)
        
        # Generate 6-digit OTP
        otp = ''.join(random.choices(string.digits, k=6))
        
        # Store OTP in database
        otp_doc = {
            "email": email,
            "otp": otp,
            "type": "verification",
            "created_at": datetime.utcnow(),
            "expires_at": datetime.utcnow() + timedelta(minutes=10)
        }
        
        self.otp_collection.insert_one(otp_doc)
        
        # Send OTP via email
        from app.services.email_service import EmailService
        email_service = EmailService()
        await email_service.send_otp_email(email, otp)
        
        return otp

    async def verify_otp(self, email: str, otp: str):
        # Find valid OTP
        otp_doc = self.otp_collection.find_one({
            "email": email,
            "otp": otp,
            "type": "verification",
            "expires_at": {"$gt": datetime.utcnow()}
        })
        
        if not otp_doc:
            raise OTPError("Invalid or expired OTP", otp_type="verification")
        
        # Find pending user
        pending_user = self.pending_users_collection.find_one({
            "email": email,
            "expires_at": {"$gt": datetime.utcnow()}
        })
        
        if not pending_user:
            raise UserNotFoundError("No pending registration found or registration expired", identifier=email)
        
        # Create the actual user account now
        user_doc = {
            "user_id": pending_user["user_id"],
            "username": pending_user["username"],
            "email": pending_user["email"],
            "mobile": pending_user["mobile"],
            "password_hash": pending_user["password_hash"],
            "first_name": pending_user["first_name"],
            "last_name": pending_user["last_name"],
            "user_type": pending_user["user_type"],
            "is_verified": True,  # Account is verified upon creation
            "created_at": datetime.utcnow()
        }
        
        # Add type-specific fields
        if pending_user["user_type"] == "patient":
            user_doc["is_pregnant"] = pending_user.get("is_pregnant", False)
        elif pending_user["user_type"] == "doctor":
            user_doc["specialization"] = pending_user.get("specialization")
        
        # Insert into appropriate collection based on user type
        if pending_user["user_type"] == "patient":
            self.patients_collection.insert_one(user_doc)
        else:  # doctor
            self.doctors_collection.insert_one(user_doc)
        
        # Remove from pending users
        self.pending_users_collection.delete_one({"_id": pending_user["_id"]})
        
        # Delete used OTP
        self.otp_collection.delete_one({"_id": otp_doc["_id"]})
        
        return {
            "message": "Account created and verified successfully!", 
            "user_id": pending_user["user_id"],
            "user_type": pending_user["user_type"],
            "status": "verified"
        }

    async def generate_password_reset_otp(self, email: str):
        # Check if user exists (check both collections)
        user = self.patients_collection.find_one({"email": email})
        if not user:
            user = self.doctors_collection.find_one({"email": email})
        
        if not user:
            raise UserNotFoundError("User not found", identifier=email)
        
        # Generate 6-digit OTP
        otp = ''.join(random.choices(string.digits, k=6))
        
        # Store OTP in database
        otp_doc = {
            "email": email,
            "otp": otp,
            "type": "password_reset",
            "created_at": datetime.utcnow(),
            "expires_at": datetime.utcnow() + timedelta(minutes=10)
        }
        
        self.otp_collection.insert_one(otp_doc)
        
        # Send password reset OTP via email
        from app.services.email_service import EmailService
        email_service = EmailService()
        await email_service.send_password_reset_email(email, otp)
        
        return otp

    async def reset_password(self, email: str, otp: str, new_password: str):
        # Find valid OTP
        otp_doc = self.otp_collection.find_one({
            "email": email,
            "otp": otp,
            "type": "password_reset",
            "expires_at": {"$gt": datetime.utcnow()}
        })
        
        if not otp_doc:
            raise Exception("Invalid or expired OTP")
        
        # Hash new password
        hashed_password = hash_password(new_password)
        
        # Update password (update in appropriate collection)
        result = self.patients_collection.update_one(
            {"email": email},
            {"$set": {"password_hash": hashed_password}}
        )
        
        # If not updated in patients, try doctors
        if result.modified_count == 0:
            self.doctors_collection.update_one(
                {"email": email},
                {"$set": {"password_hash": hashed_password}}
            )
        
        # Delete used OTP
        self.otp_collection.delete_one({"_id": otp_doc["_id"]})
        
        return {"message": "Password reset successfully"}

    async def validate_token(self, token: str):
        """Validate JWT token and check session"""
        from app.utils.security import verify_token
        
        # Decode JWT token
        payload = verify_token(token)
        if not payload:
            return {"valid": False, "error": "Invalid token"}
        
        # Check if session exists and is active
        session_id = payload.get("session_id")
        if session_id:
            session = self.sessions_collection.find_one({
                "session_id": session_id,
                "is_active": True,
                "expires_at": {"$gt": datetime.utcnow()}
            })
            
            if not session:
                return {"valid": False, "error": "Session expired or invalid"}
            
            # Update last activity
            self.sessions_collection.update_one(
                {"session_id": session_id},
                {"$set": {"last_activity": datetime.utcnow()}}
            )
        
        return {"valid": True, "user_id": payload.get("sub"), "user_type": payload.get("user_type")}
    
    async def logout_user(self, session_id: str):
        """Logout user by deactivating session"""
        result = self.sessions_collection.update_one(
            {"session_id": session_id},
            {"$set": {"is_active": False, "logout_time": datetime.utcnow()}}
        )
        
        if result.modified_count > 0:
            return {"message": "Logged out successfully"}
        else:
            return {"message": "Session not found or already logged out"}
    
    async def get_user_sessions(self, user_id: str):
        """Get all active sessions for a user"""
        sessions = list(self.sessions_collection.find({
            "user_id": user_id,
            "is_active": True,
            "expires_at": {"$gt": datetime.utcnow()}
        }))
        
        return sessions
    
    async def logout_all_sessions(self, user_id: str):
        """Logout all sessions for a user"""
        result = self.sessions_collection.update_many(
            {"user_id": user_id, "is_active": True},
            {"$set": {"is_active": False, "logout_time": datetime.utcnow()}}
        )
        
        return {"message": f"Logged out {result.modified_count} sessions"}

    def _generate_user_id(self, user_type: str):
        # Generate unique user ID based on type
        while True:
            if user_type == "patient":
                user_id = "PAT" + ''.join(random.choices(string.digits, k=8))
            elif user_type == "doctor":
                user_id = "DOC" + ''.join(random.choices(string.digits, k=8))
            else:
                raise Exception("Invalid user type")
                
            # Check if ID already exists (check all collections including pending)
            patient_exists = self.patients_collection.find_one({"user_id": user_id})
            doctor_exists = self.doctors_collection.find_one({"user_id": user_id})
            pending_exists = self.pending_users_collection.find_one({"user_id": user_id})
            
            if not patient_exists and not doctor_exists and not pending_exists:
                return user_id
