import json
import os
import secrets
from typing import Optional
from dotenv import load_dotenv, set_key

class Config:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Load JSON configuration
        try:
            with open("config.json", 'r') as f:
                self._config = json.load(f)
        except FileNotFoundError:
            # Default configuration if file doesn't exist
            self._config = {
                "database": {
                    "database_name": "patients_db",
                    "collections": {
                        "patients": "patients_v2",
                        "doctors": "doctor_v2",
                        "pending_users": "pending_users",
                        "otp_codes": "otp_codes",
                        "user_sessions": "user_sessions"
                    }
                },
                "jwt": {"algorithm": "HS256", "expire_minutes": 30},
                "email": {"smtp_host": "smtp.gmail.com", "smtp_port": 587},
                "service": {"port": 5001, "host": "0.0.0.0", "debug": False}
            }
    
    def get(self, key_path: str, default=None):
        """Get configuration value using dot notation"""
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value
    
    # Database settings
    @property
    def mongo_uri(self) -> str:
        return os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    
    @property
    def database_name(self) -> str:
        return self.get('database.database_name', 'patients_db')
    
    # JWT settings
    @property
    def jwt_secret(self) -> str:
        # Check if JWT_SECRET exists in environment
        jwt_secret = os.getenv('JWT_SECRET')
        
        if not jwt_secret or jwt_secret == 'your-secret-key-change-in-production':
            # Generate a secure random JWT secret
            jwt_secret = secrets.token_urlsafe(32)
            
            # Save to .env file if it exists
            if os.path.exists('.env'):
                set_key('.env', 'JWT_SECRET', jwt_secret)
                print(f"🔐 Generated new JWT secret and saved to .env file")
            else:
                print(f"🔐 Generated JWT secret: {jwt_secret}")
                print("⚠️  Add this to your .env file: JWT_SECRET={jwt_secret}")
        
        return jwt_secret
    
    @property
    def jwt_algorithm(self) -> str:
        return self.get('jwt.algorithm', 'HS256')
    
    @property
    def jwt_expire_minutes(self) -> int:
        return self.get('jwt.expire_minutes', 30)
    
    # Email settings
    @property
    def smtp_host(self) -> str:
        return self.get('email.smtp_host', 'smtp.gmail.com')
    
    @property
    def smtp_port(self) -> int:
        return self.get('email.smtp_port', 587)
    
    @property
    def smtp_user(self) -> Optional[str]:
        return os.getenv('SMTP_USER')
    
    @property
    def smtp_password(self) -> Optional[str]:
        return os.getenv('SMTP_PASSWORD')
    
    # Service settings
    @property
    def port(self) -> int:
        return int(os.getenv('PORT', self.get('service.port', 5001)))
    
    @property
    def host(self) -> str:
        return self.get('service.host', '0.0.0.0')
    
    @property
    def debug(self) -> bool:
        debug_env = os.getenv('DEBUG', '').lower()
        if debug_env in ['true', '1', 'yes']:
            return True
        return self.get('service.debug', False)
    
    # Collection name properties
    @property
    def patients_collection_name(self) -> str:
        return self.get('database.collections.patients', 'patients_v2')

    @property
    def doctors_collection_name(self) -> str:
        return self.get('database.collections.doctors', 'doctor_v2')

    @property
    def pending_users_collection_name(self) -> str:
        return self.get('database.collections.pending_users', 'pending_users')

    @property
    def otp_codes_collection_name(self) -> str:
        return self.get('database.collections.otp_codes', 'otp_codes')

    @property
    def user_sessions_collection_name(self) -> str:
        return self.get('database.collections.user_sessions', 'user_sessions')

settings = Config()