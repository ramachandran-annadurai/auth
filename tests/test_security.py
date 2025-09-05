import pytest
from unittest.mock import patch
import jwt
from datetime import datetime, timedelta
from app.utils.security import hash_password, verify_password, create_access_token, verify_token

class TestSecurity:
    
    def test_hash_password(self):
        """Test password hashing"""
        password = "test_password_123"
        hashed = hash_password(password)
        
        # Verify hash is created
        assert hashed is not None
        assert len(hashed) > 0
        assert hashed != password  # Should not be plain text
        assert hashed.startswith('$2b$')  # bcrypt format
    
    def test_hash_password_different_salts(self):
        """Test that same password produces different hashes"""
        password = "same_password"
        hash1 = hash_password(password)
        hash2 = hash_password(password)
        
        # Should be different due to different salts
        assert hash1 != hash2
    
    def test_verify_password_correct(self):
        """Test password verification with correct password"""
        password = "correct_password"
        hashed = hash_password(password)
        
        result = verify_password(password, hashed)
        assert result == True
    
    def test_verify_password_incorrect(self):
        """Test password verification with incorrect password"""
        correct_password = "correct_password"
        wrong_password = "wrong_password"
        hashed = hash_password(correct_password)
        
        result = verify_password(wrong_password, hashed)
        assert result == False
    
    def test_verify_password_empty(self):
        """Test password verification with empty password"""
        password = "test_password"
        hashed = hash_password(password)
        
        result = verify_password("", hashed)
        assert result == False
    
    @patch('app.utils.security.settings')
    def test_create_access_token(self, mock_settings):
        """Test JWT token creation"""
        mock_settings.jwt_secret = "test_secret"
        mock_settings.jwt_algorithm = "HS256"
        mock_settings.jwt_expire_minutes = 30
        
        data = {"sub": "PAT12345678", "user_type": "patient"}
        token = create_access_token(data)
        
        assert token is not None
        assert len(token) > 0
        assert isinstance(token, str)
        
        # Verify token can be decoded
        decoded = jwt.decode(token, "test_secret", algorithms=["HS256"])
        assert decoded["sub"] == "PAT12345678"
        assert decoded["user_type"] == "patient"
        assert "exp" in decoded
    
    @patch('app.utils.security.settings')
    def test_create_access_token_doctor(self, mock_settings):
        """Test JWT token creation for doctor"""
        mock_settings.jwt_secret = "test_secret"
        mock_settings.jwt_algorithm = "HS256"
        mock_settings.jwt_expire_minutes = 30
        
        data = {"sub": "DOC87654321", "user_type": "doctor"}
        token = create_access_token(data)
        
        decoded = jwt.decode(token, "test_secret", algorithms=["HS256"])
        assert decoded["sub"] == "DOC87654321"
        assert decoded["user_type"] == "doctor"
    
    @patch('app.utils.security.settings')
    def test_verify_token_valid(self, mock_settings):
        """Test JWT token verification with valid token"""
        mock_settings.jwt_secret = "test_secret"
        mock_settings.jwt_algorithm = "HS256"
        mock_settings.jwt_expire_minutes = 30
        
        # Create token
        data = {"sub": "PAT12345678", "user_type": "patient"}
        token = create_access_token(data)
        
        # Verify token
        payload = verify_token(token)
        
        assert payload is not None
        assert payload["sub"] == "PAT12345678"
        assert payload["user_type"] == "patient"
    
    @patch('app.utils.security.settings')
    def test_verify_token_invalid(self, mock_settings):
        """Test JWT token verification with invalid token"""
        mock_settings.jwt_secret = "test_secret"
        mock_settings.jwt_algorithm = "HS256"
        
        invalid_token = "invalid.jwt.token"
        payload = verify_token(invalid_token)
        
        assert payload is None
    
    @patch('app.utils.security.settings')
    def test_verify_token_expired(self, mock_settings):
        """Test JWT token verification with expired token"""
        mock_settings.jwt_secret = "test_secret"
        mock_settings.jwt_algorithm = "HS256"
        
        # Create expired token
        expired_data = {
            "sub": "PAT12345678",
            "exp": datetime.utcnow() - timedelta(minutes=1)  # Expired 1 minute ago
        }
        expired_token = jwt.encode(expired_data, "test_secret", algorithm="HS256")
        
        payload = verify_token(expired_token)
        assert payload is None
    
    @patch('app.utils.security.settings')
    def test_verify_token_wrong_secret(self, mock_settings):
        """Test JWT token verification with wrong secret"""
        mock_settings.jwt_secret = "wrong_secret"
        mock_settings.jwt_algorithm = "HS256"
        
        # Create token with different secret
        correct_secret = "correct_secret"
        data = {"sub": "PAT12345678"}
        token = jwt.encode(data, correct_secret, algorithm="HS256")
        
        payload = verify_token(token)
        assert payload is None
    
    def test_password_security_requirements(self):
        """Test that password hashing meets security requirements"""
        password = "user_password_123"
        hashed = hash_password(password)
        
        # Verify bcrypt format and cost
        assert hashed.startswith('$2b$12$')  # bcrypt with cost 12
        assert len(hashed) == 60  # Standard bcrypt hash length
        
        # Verify it's not reversible
        assert password not in hashed
        assert verify_password(password, hashed) == True
