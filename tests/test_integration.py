import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock
from app.main import app

class TestIntegration:
    """Integration tests for complete authentication flows"""
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    @patch('app.services.auth_service.get_database')
    @patch('app.services.email_service.EmailService.send_otp_email')
    def test_complete_patient_registration_flow(self, mock_email, mock_db, client):
        """Test complete patient registration and verification flow"""
        # Mock database
        mock_collection = Mock()
        mock_otp_collection = Mock()
        mock_db.return_value = {
            "patients_v2": mock_collection,
            "otp_codes": mock_otp_collection
        }
        
        # Step 1: Register patient
        mock_collection.find_one.return_value = None  # No existing user
        mock_collection.insert_one.return_value = Mock()
        
        patient_data = {
            "username": "integration_patient",
            "email": "integration@test.com",
            "mobile": "1234567890",
            "password": "password123",
            "first_name": "Integration",
            "last_name": "Patient",
            "user_type": "patient",
            "is_pregnant": True
        }
        
        response = client.post("/auth/register", json=patient_data)
        assert response.status_code == 200
        assert response.json()["success"] == True
        
        # Step 2: Send OTP
        mock_email.return_value = None
        mock_otp_collection.insert_one.return_value = Mock()
        
        otp_request = {"email": "integration@test.com"}
        response = client.post("/auth/send-otp", json=otp_request)
        assert response.status_code == 200
        
        # Step 3: Verify OTP
        mock_otp_collection.find_one.return_value = {
            "_id": "otp_id",
            "email": "integration@test.com",
            "otp": "123456"
        }
        mock_collection.update_one.return_value = Mock()
        mock_otp_collection.delete_one.return_value = Mock()
        
        otp_verify = {"email": "integration@test.com", "otp": "123456"}
        response = client.post("/auth/verify-otp", json=otp_verify)
        assert response.status_code == 200
        
        # Step 4: Login
        mock_user = {
            "user_id": "PAT12345678",
            "username": "integration_patient",
            "email": "integration@test.com",
            "user_type": "patient",
            "password_hash": "$2b$12$mocked_hash",
            "is_verified": True
        }
        mock_collection.find_one.return_value = mock_user
        
        with patch('app.services.auth_service.verify_password', return_value=True), \
             patch('app.services.auth_service.create_access_token', return_value="jwt_token"):
            
            login_data = {"username": "PAT12345678", "password": "password123"}
            response = client.post("/auth/login", json=login_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["user_id"] == "PAT12345678"
            assert data["user_type"] == "patient"
            assert data["access_token"] == "jwt_token"
    
    @patch('app.services.auth_service.get_database')
    @patch('app.services.email_service.EmailService.send_otp_email')
    def test_complete_doctor_registration_flow(self, mock_email, mock_db, client):
        """Test complete doctor registration and verification flow"""
        # Mock database
        mock_collection = Mock()
        mock_otp_collection = Mock()
        mock_db.return_value = {
            "patients_v2": mock_collection,
            "otp_codes": mock_otp_collection
        }
        
        # Step 1: Register doctor
        mock_collection.find_one.return_value = None
        mock_collection.insert_one.return_value = Mock()
        
        doctor_data = {
            "username": "integration_doctor",
            "email": "doctor@integration.com",
            "mobile": "0987654321",
            "password": "password123",
            "first_name": "Dr. Integration",
            "last_name": "Doctor",
            "user_type": "doctor",
            "specialization": "Integration Medicine"
        }
        
        response = client.post("/auth/register", json=doctor_data)
        assert response.status_code == 200
        
        # Step 2: Login after verification
        mock_user = {
            "user_id": "DOC87654321",
            "username": "integration_doctor",
            "user_type": "doctor",
            "password_hash": "$2b$12$mocked_hash",
            "is_verified": True
        }
        mock_collection.find_one.return_value = mock_user
        
        with patch('app.services.auth_service.verify_password', return_value=True), \
             patch('app.services.auth_service.create_access_token', return_value="doctor_jwt_token"):
            
            login_data = {"username": "DOC87654321", "password": "password123"}
            response = client.post("/auth/login", json=login_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["user_id"] == "DOC87654321"
            assert data["user_type"] == "doctor"
    
    @patch('app.services.auth_service.get_database')
    @patch('app.services.email_service.EmailService.send_password_reset_email')
    def test_complete_password_reset_flow(self, mock_email, mock_db, client):
        """Test complete password reset flow"""
        # Mock database
        mock_collection = Mock()
        mock_otp_collection = Mock()
        mock_db.return_value = {
            "patients_v2": mock_collection,
            "otp_codes": mock_otp_collection
        }
        
        # Step 1: Request password reset
        mock_collection.find_one.return_value = {"email": "reset@test.com"}
        mock_otp_collection.insert_one.return_value = Mock()
        mock_email.return_value = None
        
        forgot_request = {"email": "reset@test.com"}
        response = client.post("/auth/forgot-password", json=forgot_request)
        assert response.status_code == 200
        
        # Step 2: Reset password with OTP
        mock_otp_collection.find_one.return_value = {
            "_id": "reset_otp_id",
            "email": "reset@test.com",
            "otp": "654321",
            "type": "password_reset"
        }
        mock_collection.update_one.return_value = Mock()
        mock_otp_collection.delete_one.return_value = Mock()
        
        reset_data = {
            "email": "reset@test.com",
            "otp": "654321",
            "new_password": "newpassword123"
        }
        response = client.post("/auth/reset-password", json=reset_data)
        assert response.status_code == 200
        assert "reset successfully" in response.json()["message"]
    
    @patch('app.services.auth_service.get_database')
    def test_login_with_different_identifiers(self, mock_db, client):
        """Test login with different identifier types (ID, email, username)"""
        # Mock database
        mock_collection = Mock()
        mock_db.return_value = {"patients_v2": mock_collection, "otp_codes": Mock()}
        
        mock_user = {
            "user_id": "PAT12345678",
            "username": "test_patient",
            "email": "patient@test.com",
            "user_type": "patient",
            "password_hash": "$2b$12$mocked_hash",
            "is_verified": True
        }
        
        with patch('app.services.auth_service.verify_password', return_value=True), \
             patch('app.services.auth_service.create_access_token', return_value="test_token"):
            
            # Test login with Patient ID
            mock_collection.find_one.return_value = mock_user
            response = client.post("/auth/login", json={"username": "PAT12345678", "password": "pass"})
            assert response.status_code == 200
            
            # Test login with email
            mock_collection.find_one.return_value = mock_user
            response = client.post("/auth/login", json={"username": "patient@test.com", "password": "pass"})
            assert response.status_code == 200
            
            # Test login with username
            mock_collection.find_one.return_value = mock_user
            response = client.post("/auth/login", json={"username": "test_patient", "password": "pass"})
            assert response.status_code == 200
    
    def test_api_documentation_accessible(self, client):
        """Test that API documentation is accessible"""
        response = client.get("/docs")
        assert response.status_code == 200
        
        response = client.get("/redoc")
        assert response.status_code == 200
    
    def test_openapi_schema(self, client):
        """Test OpenAPI schema generation"""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        
        schema = response.json()
        assert "openapi" in schema
        assert "paths" in schema
        assert "/auth/register" in schema["paths"]
        assert "/auth/login" in schema["paths"]
    
    @patch('app.services.auth_service.get_database')
    def test_error_handling_flow(self, mock_db, client):
        """Test error handling in authentication flow"""
        mock_collection = Mock()
        mock_db.return_value = {"patients_v2": mock_collection, "otp_codes": Mock()}
        
        # Test registration with existing user
        mock_collection.find_one.return_value = {"email": "existing@test.com"}
        
        existing_user_data = {
            "username": "existing_user",
            "email": "existing@test.com",
            "mobile": "1234567890",
            "password": "password123",
            "first_name": "Existing",
            "last_name": "User",
            "user_type": "patient"
        }
        
        response = client.post("/auth/register", json=existing_user_data)
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]
        
        # Test login with non-existent user
        mock_collection.find_one.return_value = None
        
        response = client.post("/auth/login", json={"username": "nonexistent", "password": "pass"})
        assert response.status_code == 401
        assert "Invalid credentials" in response.json()["detail"]
