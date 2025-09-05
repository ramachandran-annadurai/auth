from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime
from app.services.auth_service import AuthService

router = APIRouter()
auth_service = AuthService()

@router.get("/pending-users")
async def get_pending_users(
    email: Optional[str] = Query(None, description="Filter by email"),
    user_type: Optional[str] = Query(None, description="Filter by user_type (patient/doctor)"),
    include_expired: bool = Query(False, description="Include expired pending registrations")
):
    """Get all pending user registrations for admin review"""
    try:
        # Build filter query
        filter_query = {}
        
        if email:
            filter_query["email"] = {"$regex": email, "$options": "i"}  # Case insensitive search
        
        if user_type:
            filter_query["user_type"] = user_type
        
        if not include_expired:
            filter_query["expires_at"] = {"$gt": datetime.utcnow()}
        
        # Get pending users from database
        pending_users = list(auth_service.pending_users_collection.find(
            filter_query,
            {
                "password_hash": 0  # Exclude password hash for security
            }
        ).sort("created_at", -1))  # Latest first
        
        # Convert ObjectId to string and format dates
        for user in pending_users:
            user["_id"] = str(user["_id"])
            user["created_at"] = user["created_at"].isoformat()
            user["expires_at"] = user["expires_at"].isoformat()
            
            # Add status information
            user["status"] = "expired" if user["expires_at"] < datetime.utcnow().isoformat() else "pending"
            user["time_remaining"] = None
            
            if user["status"] == "pending":
                expires_dt = datetime.fromisoformat(user["expires_at"])
                remaining = expires_dt - datetime.utcnow()
                user["time_remaining"] = f"{remaining.total_seconds() / 60:.1f} minutes"
        
        return {
            "pending_users": pending_users,
            "total_count": len(pending_users),
            "active_count": len([u for u in pending_users if u["status"] == "pending"]),
            "expired_count": len([u for u in pending_users if u["status"] == "expired"])
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch pending users: {str(e)}")

@router.delete("/pending-users/{user_id}")
async def delete_pending_user(user_id: str):
    """Delete a specific pending user registration (admin only)"""
    try:
        result = auth_service.pending_users_collection.delete_one({"user_id": user_id})
        
        if result.deleted_count > 0:
            return {"message": f"Pending user {user_id} deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Pending user not found")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete pending user: {str(e)}")

@router.post("/pending-users/{user_id}/resend-otp")
async def admin_resend_otp(user_id: str):
    """Resend OTP for a pending user (admin function)"""
    try:
        # Find pending user
        pending_user = auth_service.pending_users_collection.find_one({"user_id": user_id})
        
        if not pending_user:
            raise HTTPException(status_code=404, detail="Pending user not found")
        
        if pending_user["expires_at"] < datetime.utcnow():
            raise HTTPException(status_code=400, detail="Pending registration has expired")
        
        # Generate and send new OTP
        otp = await auth_service.generate_otp(pending_user["email"])
        
        return {
            "message": f"OTP resent to {pending_user['email']}",
            "user_id": user_id,
            "email": pending_user["email"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to resend OTP: {str(e)}")

@router.get("/collections-status")
async def get_collections_status():
    """Get status of all collections for admin dashboard"""
    try:
        from app.utils.config import settings
        
        # Count documents in each collection
        patients_count = auth_service.patients_collection.count_documents({})
        doctors_count = auth_service.doctors_collection.count_documents({})
        pending_count = auth_service.pending_users_collection.count_documents({})
        pending_active = auth_service.pending_users_collection.count_documents({
            "expires_at": {"$gt": datetime.utcnow()}
        })
        sessions_count = auth_service.sessions_collection.count_documents({"is_active": True})
        otp_count = auth_service.otp_collection.count_documents({
            "expires_at": {"$gt": datetime.utcnow()}
        })
        
        return {
            "collections": {
                settings.patients_collection_name: {
                    "count": patients_count,
                    "description": "Verified patient accounts"
                },
                settings.doctors_collection_name: {
                    "count": doctors_count,
                    "description": "Verified doctor accounts"
                },
                settings.pending_users_collection_name: {
                    "total_count": pending_count,
                    "active_count": pending_active,
                    "expired_count": pending_count - pending_active,
                    "description": "Unverified registrations (temporary)"
                },
                settings.user_sessions_collection_name: {
                    "count": sessions_count,
                    "description": "Active user sessions"
                },
                settings.otp_codes_collection_name: {
                    "count": otp_count,
                    "description": "Active OTP codes"
                }
            },
            "total_verified_users": patients_count + doctors_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get collections status: {str(e)}")
