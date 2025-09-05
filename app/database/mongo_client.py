import pymongo
from app.utils.config import settings

_client = None
_database = None

def get_database():
    global _client, _database
    
    if _database is None:
        try:
            _client = pymongo.MongoClient(settings.mongo_uri)
            _database = _client[settings.database_name]
            
            # Create indexes safely (skip if already exists with different options)
            _create_indexes_safely(_database)
            
        except Exception as e:
            raise Exception(f"Database connection failed: {str(e)}")
    
    return _database

def _create_indexes_safely(database):
    """Create indexes safely, handling conflicts gracefully"""
    try:
        # Clean old conflicting indexes first
        _clean_old_indexes(database)
        
        # Get existing indexes to avoid conflicts
        existing_indexes = {}
        
        # Check patients collection indexes
        try:
            patients_indexes = database[settings.patients_collection_name].list_indexes()
            existing_indexes["patients"] = [idx["name"] for idx in patients_indexes]
        except:
            existing_indexes["patients"] = []
        
        # Check doctors collection indexes
        try:
            doctors_indexes = database[settings.doctors_collection_name].list_indexes()
            existing_indexes["doctors"] = [idx["name"] for idx in doctors_indexes]
        except:
            existing_indexes["doctors"] = []
        
        # Check otp_codes collection indexes
        try:
            otp_indexes = database[settings.otp_codes_collection_name].list_indexes()
            existing_indexes["otp_codes"] = [idx["name"] for idx in otp_indexes]
        except:
            existing_indexes["otp_codes"] = []
        
        # Check user_sessions collection indexes
        try:
            session_indexes = database[settings.user_sessions_collection_name].list_indexes()
            existing_indexes["user_sessions"] = [idx["name"] for idx in session_indexes]
        except:
            existing_indexes["user_sessions"] = []
        
        # Check pending_users collection indexes
        try:
            pending_indexes = database[settings.pending_users_collection_name].list_indexes()
            existing_indexes["pending_users"] = [idx["name"] for idx in pending_indexes]
        except:
            existing_indexes["pending_users"] = []
        
        # Create patients collection indexes with custom names to avoid conflicts
        if "patients_email_unique_idx" not in existing_indexes["patients"]:
            try:
                database[settings.patients_collection_name].create_index("email", unique=True, name="patients_email_unique_idx")
                print("✅ Created patients email index")
            except pymongo.errors.OperationFailure:
                # Silently skip if exists - no warning needed
                pass
        
        if "patients_username_unique_idx" not in existing_indexes["patients"]:
            try:
                database[settings.patients_collection_name].create_index("username", unique=True, name="patients_username_unique_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "patients_user_id_unique_idx" not in existing_indexes["patients"]:
            try:
                database[settings.patients_collection_name].create_index("user_id", unique=True, name="patients_user_id_unique_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "patients_user_type_idx" not in existing_indexes["patients"]:
            try:
                database[settings.patients_collection_name].create_index("user_type", name="patients_user_type_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        # Create doctors collection indexes with custom names to avoid conflicts
        if "doctors_email_unique_idx" not in existing_indexes["doctors"]:
            try:
                database[settings.doctors_collection_name].create_index("email", unique=True, name="doctors_email_unique_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "doctors_username_unique_idx" not in existing_indexes["doctors"]:
            try:
                database[settings.doctors_collection_name].create_index("username", unique=True, name="doctors_username_unique_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "doctors_user_id_unique_idx" not in existing_indexes["doctors"]:
            try:
                database[settings.doctors_collection_name].create_index("user_id", unique=True, name="doctors_user_id_unique_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "doctors_user_type_idx" not in existing_indexes["doctors"]:
            try:
                database[settings.doctors_collection_name].create_index("user_type", name="doctors_user_type_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "doctors_specialization_idx" not in existing_indexes["doctors"]:
            try:
                database[settings.doctors_collection_name].create_index("specialization", name="doctors_specialization_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        # Create otp_codes indexes
        if "otp_expires_idx" not in existing_indexes["otp_codes"]:
            try:
                database[settings.otp_codes_collection_name].create_index("expires_at", expireAfterSeconds=0, name="otp_expires_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        # Create user_sessions indexes
        if "session_id_unique_idx" not in existing_indexes["user_sessions"]:
            try:
                database[settings.user_sessions_collection_name].create_index("session_id", unique=True, name="session_id_unique_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "session_user_id_idx" not in existing_indexes["user_sessions"]:
            try:
                database[settings.user_sessions_collection_name].create_index("user_id", name="session_user_id_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "session_expires_idx" not in existing_indexes["user_sessions"]:
            try:
                database[settings.user_sessions_collection_name].create_index("expires_at", expireAfterSeconds=0, name="session_expires_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        # Compound index for user_id + is_active
        if "user_active_sessions_idx" not in existing_indexes["user_sessions"]:
            try:
                database[settings.user_sessions_collection_name].create_index([("user_id", 1), ("is_active", 1)], name="user_active_sessions_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        # Create pending_users indexes
        if "pending_email_idx" not in existing_indexes["pending_users"]:
            try:
                database[settings.pending_users_collection_name].create_index("email", name="pending_email_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "pending_username_idx" not in existing_indexes["pending_users"]:
            try:
                database[settings.pending_users_collection_name].create_index("username", name="pending_username_idx")
            except pymongo.errors.OperationFailure:
                pass
        
        if "pending_expires_idx" not in existing_indexes["pending_users"]:
            try:
                database[settings.pending_users_collection_name].create_index("expires_at", expireAfterSeconds=0, name="pending_expires_idx")
            except pymongo.errors.OperationFailure:
                pass
            
    except Exception as e:
        # Silently continue - indexes will be created as needed
        pass

def _clean_old_indexes(database):
    """Clean old conflicting indexes to prevent warnings"""
    try:
        # Old index names that might conflict
        old_indexes_to_remove = [
            "email_1", 
            "username_1", 
            "user_id_1", 
            "user_type_1",
            "patient_id_1",
            "mobile_1"
        ]
        
        # Collections to clean
        collections_to_clean = [
            settings.patients_collection_name,
            settings.doctors_collection_name
        ]
        
        for collection_name in collections_to_clean:
            collection = database[collection_name]
            
            # Get current indexes
            try:
                current_indexes = [idx["name"] for idx in collection.list_indexes()]
                
                # Remove conflicting old indexes
                for old_index in old_indexes_to_remove:
                    if old_index in current_indexes:
                        try:
                            collection.drop_index(old_index)
                            print(f"🧹 Cleaned old index: {old_index} from {collection_name}")
                        except Exception:
                            # Index might be in use or already dropped
                            pass
                            
            except Exception:
                # Collection might not exist yet
                pass
                
    except Exception:
        # Cleanup failed, but continue with index creation
        pass

def close_database():
    global _client
    if _client:
        _client.close()
