import pymongo
from urllib.parse import quote_plus
from app.utils.config import settings

_client = None
_database = None

def _encode_mongo_uri(uri: str) -> str:
    """
    Properly encode MongoDB URI to handle special characters in username/password.
    This is required for RFC 3986 compliance.
    """
    try:
        # If the URI already contains encoded characters, return as is
        if '%' in uri and ('@' not in uri or uri.count('@') == 1):
            return uri
        
        # Parse the URI to extract components
        if '://' not in uri:
            return uri
            
        protocol, rest = uri.split('://', 1)
        
        # Check if there's authentication info
        if '@' in rest:
            auth_part, host_part = rest.split('@', 1)
            
            # Check if auth part contains username:password
            if ':' in auth_part:
                username, password = auth_part.split(':', 1)
                # URL encode the username and password
                encoded_username = quote_plus(username)
                encoded_password = quote_plus(password)
                encoded_auth = f"{encoded_username}:{encoded_password}"
            else:
                # Only username provided
                encoded_auth = quote_plus(auth_part)
            
            return f"{protocol}://{encoded_auth}@{host_part}"
        else:
            # No authentication, return as is
            return uri
            
    except Exception as e:
        # If encoding fails, return original URI and let pymongo handle the error
        print(f"Warning: Failed to encode MongoDB URI: {e}")
        return uri

def get_database():
    global _client, _database
    
    if _database is None:
        try:
            # Encode the MongoDB URI to handle special characters
            encoded_uri = _encode_mongo_uri(settings.mongo_uri)
            _client = pymongo.MongoClient(encoded_uri)
            _database = _client[settings.database_name]
            
            # Create indexes safely (skip if already exists with different options)
            _create_indexes_safely(_database)
            
        except Exception as e:
            error_msg = f"Database connection failed: {str(e)}"
            print(f"❌ {error_msg}")
            print(f"🔍 MongoDB URI (first 50 chars): {settings.mongo_uri[:50]}...")
            print(f"🔍 Encoded URI (first 50 chars): {encoded_uri[:50]}...")
            raise Exception(error_msg)
    
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
