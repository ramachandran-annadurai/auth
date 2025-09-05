import pytest
import asyncio
import sys
import os
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

# Add the parent directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.auth_service import AuthService
from app.models.auth_models import UserRegister

class TestAuthService:
    
    @pytest.fixture
    def auth_service(self):
        """Create AuthService instance with mocked database"""
        with patch('app.services.auth_service.get_database') as mock_db:
            mock_collection = Mock()
            mock_otp_collection = Mock()
            mock_db.return_value = {
                "patients_v2": mock_collection,
                "otp_codes": mock_otp_collection
            }
            service = AuthService()
            service.users_collection = mock_collection
            service.otp_collection = mock_otp_collection
            return service
    
    @pytest.mark.asyncio
    async def test_register_patient_success(self, auth_service):
        """Test successful patient registration"""
        # Mock database responses
        auth_service.users_collection.find_one.return_value = None  # No existing user
        auth_service.users_collection.insert_one.return_value = Mock()
        
        # Test data
        patient_data = UserRegister(
            username="test_patient",
            email="patient@test.com",
            mobile="1234567890",
            password="password123",
            first_name="Test",
            last_name="Patient",
            user_type="patient",
            is_pregnant=False
        )
        
        # Execute
        result = await auth_service.register_user(patient_data)
        
        # Assertions
        assert result["message"] == "User registered successfully. Please verify OTP."
        assert "user_id" in result
        assert result["user_id"].startswith("PAT")
        assert len(result["user_id"]) == 11  # PAT + 8 digits
        
        # Verify database calls
        auth_service.users_collection.find_one.assert_called_once()
        auth_service.users_collection.insert_one.assert_called_once()
        
        # Verify inserted document structure
        inserted_doc = auth_service.users_collection.insert_one.call_args[0][0]
        assert inserted_doc["user_type"] == "patient"
        assert inserted_doc["is_pregnant"] == False
        assert "specialization" not in inserted_doc
    
    @pytest.mark.asyncio
    async def test_register_doctor_success(self, auth_service):
        """Test successful doctor registration"""
        # Mock database responses
        auth_service.users_collection.find_one.return_value = None
        auth_service.users_collection.insert_one.return_value = Mock()
        
        # Test data
        doctor_data = UserRegister(
            username="test_doctor",
            email="doctor@test.com",
            mobile="0987654321",
            password="password123",
            first_name="Dr. Test",
            last_name="Doctor",
            user_type="doctor",
            specialization="Cardiology"
        )
        
        # Execute
        result = await auth_service.register_user(doctor_data)
        
        # Assertions
        assert result["message"] == "User registered successfully. Please verify OTP."
        assert "user_id" in result
        assert result["user_id"].startswith("DOC")
        assert len(result["user_id"]) == 11  # DOC + 8 digits
        
        # Verify inserted document structure
        inserted_doc = auth_service.users_collection.insert_one.call_args[0][0]
        assert inserted_doc["user_type"] == "doctor"
        assert inserted_doc["specialization"] == "Cardiology"
        assert "is_pregnant" not in inserted_doc
    
    @pytest.mark.asyncio
    async def test_register_invalid_user_type(self, auth_service):
        """Test registration with invalid user type"""
        invalid_data = UserRegister(
            username="test_user",
            email="test@test.com",
            mobile="1234567890",
            password="password123",
            first_name="Test",
            last_name="User",
            user_type="invalid_type"
        )
        
        with pytest.raises(Exception) as exc_info:
            await auth_service.register_user(invalid_data)
        
        assert "Invalid user type" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_register_existing_user(self, auth_service):
        """Test registration with existing email/username"""
        # Mock existing user
        auth_service.users_collection.find_one.return_value = {"email": "existing@test.com"}
        
        user_data = UserRegister(
            username="test_user",
            email="existing@test.com",
            mobile="1234567890",
            password="password123",
            first_name="Test",
            last_name="User",
            user_type="patient"
        )
        
        with pytest.raises(Exception) as exc_info:
            await auth_service.register_user(user_data)
        
        assert "User already exists" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_authenticate_patient_success(self, auth_service):
        """Test successful patient authentication"""
        # Mock user data
        mock_user = {
            "user_id": "PAT12345678",
            "username": "test_patient",
            "email": "patient@test.com",
            "user_type": "patient",
            "password_hash": "$2b$12$mocked_hash",
            "is_verified": True
        }
        auth_service.users_collection.find_one.return_value = mock_user
        
        with patch('app.services.auth_service.verify_password', return_value=True), \
             patch('app.services.auth_service.create_access_token', return_value="mocked_token"):
            
            result = await auth_service.authenticate_user("PAT12345678", "password123")
            
            assert result["token"] == "mocked_token"
            assert result["user_id"] == "PAT12345678"
            assert result["user_type"] == "patient"
    
    @pytest.mark.asyncio
    async def test_authenticate_doctor_success(self, auth_service):
        """Test successful doctor authentication"""
        mock_user = {
            "user_id": "DOC87654321",
            "username": "test_doctor",
            "email": "doctor@test.com",
            "user_type": "doctor",
            "password_hash": "$2b$12$mocked_hash",
            "is_verified": True
        }
        auth_service.users_collection.find_one.return_value = mock_user
        
        with patch('app.services.auth_service.verify_password', return_value=True), \
             patch('app.services.auth_service.create_access_token', return_value="mocked_token"):
            
            result = await auth_service.authenticate_user("DOC87654321", "password123")
            
            assert result["token"] == "mocked_token"
            assert result["user_id"] == "DOC87654321"
            assert result["user_type"] == "doctor"
    
    @pytest.mark.asyncio
    async def test_authenticate_with_email(self, auth_service):
        """Test authentication using email address"""
        mock_user = {
            "user_id": "PAT12345678",
            "email": "patient@test.com",
            "user_type": "patient",
            "password_hash": "$2b$12$mocked_hash",
            "is_verified": True
        }
        auth_service.users_collection.find_one.return_value = mock_user
        
        with patch('app.services.auth_service.verify_password', return_value=True), \
             patch('app.services.auth_service.create_access_token', return_value="mocked_token"):
            
            result = await auth_service.authenticate_user("patient@test.com", "password123")
            
            assert result["user_id"] == "PAT12345678"
            assert result["user_type"] == "patient"
    
    @pytest.mark.asyncio
    async def test_authenticate_user_not_found(self, auth_service):
        """Test authentication with non-existent user"""
        auth_service.users_collection.find_one.return_value = None
        
        with pytest.raises(Exception) as exc_info:
            await auth_service.authenticate_user("nonexistent", "password123")
        
        assert "Invalid credentials" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_authenticate_unverified_user(self, auth_service):
        """Test authentication with unverified user"""
        mock_user = {
            "user_id": "PAT12345678",
            "password_hash": "$2b$12$mocked_hash",
            "is_verified": False
        }
        auth_service.users_collection.find_one.return_value = mock_user
        
        with pytest.raises(Exception) as exc_info:
            await auth_service.authenticate_user("PAT12345678", "password123")
        
        assert "Account not verified" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_authenticate_wrong_password(self, auth_service):
        """Test authentication with wrong password"""
        mock_user = {
            "user_id": "PAT12345678",
            "password_hash": "$2b$12$mocked_hash",
            "is_verified": True
        }
        auth_service.users_collection.find_one.return_value = mock_user
        
        with patch('app.services.auth_service.verify_password', return_value=False):
            with pytest.raises(Exception) as exc_info:
                await auth_service.authenticate_user("PAT12345678", "wrongpassword")
            
            assert "Invalid credentials" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_generate_otp_success(self, auth_service):
        """Test successful OTP generation"""
        mock_user = {"email": "test@test.com"}
        auth_service.users_collection.find_one.return_value = mock_user
        auth_service.otp_collection.insert_one.return_value = Mock()
        
        otp = await auth_service.generate_otp("test@test.com")
        
        assert len(otp) == 6
        assert otp.isdigit()
        auth_service.otp_collection.insert_one.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_generate_otp_user_not_found(self, auth_service):
        """Test OTP generation for non-existent user"""
        auth_service.users_collection.find_one.return_value = None
        
        with pytest.raises(Exception) as exc_info:
            await auth_service.generate_otp("nonexistent@test.com")
        
        assert "User not found" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_verify_otp_success(self, auth_service):
        """Test successful OTP verification"""
        mock_otp = {
            "_id": "otp_id",
            "email": "test@test.com",
            "otp": "123456",
            "expires_at": datetime.utcnow() + timedelta(minutes=5)
        }
        auth_service.otp_collection.find_one.return_value = mock_otp
        auth_service.users_collection.update_one.return_value = Mock()
        auth_service.otp_collection.delete_one.return_value = Mock()
        
        result = await auth_service.verify_otp("test@test.com", "123456")
        
        assert result["message"] == "Account verified successfully"
        auth_service.users_collection.update_one.assert_called_once()
        auth_service.otp_collection.delete_one.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_verify_otp_invalid(self, auth_service):
        """Test OTP verification with invalid/expired OTP"""
        auth_service.otp_collection.find_one.return_value = None
        
        with pytest.raises(Exception) as exc_info:
            await auth_service.verify_otp("test@test.com", "123456")
        
        assert "Invalid or expired OTP" in str(exc_info.value)
    
    def test_generate_patient_id(self, auth_service):
        """Test patient ID generation"""
        auth_service.users_collection.find_one.return_value = None  # ID doesn't exist
        
        patient_id = auth_service._generate_user_id("patient")
        
        assert patient_id.startswith("PAT")
        assert len(patient_id) == 11
        assert patient_id[3:].isdigit()
    
    def test_generate_doctor_id(self, auth_service):
        """Test doctor ID generation"""
        auth_service.users_collection.find_one.return_value = None  # ID doesn't exist
        
        doctor_id = auth_service._generate_user_id("doctor")
        
        assert doctor_id.startswith("DOC")
        assert len(doctor_id) == 11
        assert doctor_id[3:].isdigit()
    
    def test_generate_invalid_user_type_id(self, auth_service):
        """Test ID generation with invalid user type"""
        with pytest.raises(Exception) as exc_info:
            auth_service._generate_user_id("invalid")
        
        assert "Invalid user type" in str(exc_info.value)
