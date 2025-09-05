import pytest
import sys
import os

# Add the parent directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
from app.main import app

class TestAuthRoutes:
    
    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)
    
    def test_health_check(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
        assert response.json()["service"] == "Auth Service"
    
    def test_root_endpoint(self, client):
        """Test root endpoint"""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "Patient Auth Service"
        assert data["version"] == "1.0.0"
        assert data["status"] == "running"
    
    @patch('app.routes.auth_routes.auth_service.register_user')
    def test_register_patient_success(self, mock_register, client):
        """Test successful patient registration"""
        mock_register.return_value = {
            "message": "User registered successfully. Please verify OTP.",
            "user_id": "PAT12345678"
        }
        
        patient_data = {
            "username": "test_patient",
            "email": "patient@test.com",
            "mobile": "1234567890",
            "password": "password123",
            "first_name": "Test",
            "last_name": "Patient",
            "user_type": "patient",
            "is_pregnant": False
        }
        
        response = client.post("/auth/register", json=patient_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "registered successfully" in data["message"]
        mock_register.assert_called_once()
    
    @patch('app.routes.auth_routes.auth_service.register_user')
    def test_register_doctor_success(self, mock_register, client):
        """Test successful doctor registration"""
        mock_register.return_value = {
            "message": "User registered successfully. Please verify OTP.",
            "user_id": "DOC87654321"
        }
        
        doctor_data = {
            "username": "test_doctor",
            "email": "doctor@test.com",
            "mobile": "0987654321",
            "password": "password123",
            "first_name": "Dr. Test",
            "last_name": "Doctor",
            "user_type": "doctor",
            "specialization": "Cardiology"
        }
        
        response = client.post("/auth/register", json=doctor_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        mock_register.assert_called_once()
    
    @patch('app.routes.auth_routes.auth_service.register_user')
    def test_register_validation_error(self, mock_register, client):
        """Test registration with validation error"""
        # Missing required fields
        invalid_data = {
            "username": "test",
            "email": "invalid-email"  # Invalid email format
        }
        
        response = client.post("/auth/register", json=invalid_data)
        
        assert response.status_code == 422  # Validation error
        mock_register.assert_not_called()
    
    @patch('app.routes.auth_routes.auth_service.register_user')
    def test_register_service_error(self, mock_register, client):
        """Test registration with service error"""
        mock_register.side_effect = Exception("User already exists")
        
        patient_data = {
            "username": "existing_user",
            "email": "existing@test.com",
            "mobile": "1234567890",
            "password": "password123",
            "first_name": "Test",
            "last_name": "User",
            "user_type": "patient"
        }
        
        response = client.post("/auth/register", json=patient_data)
        
        assert response.status_code == 400
        assert "User already exists" in response.json()["detail"]
    
    @patch('app.routes.auth_routes.auth_service.authenticate_user')
    def test_login_patient_success(self, mock_authenticate, client):
        """Test successful patient login"""
        mock_authenticate.return_value = {
            "token": "mocked_jwt_token",
            "user_id": "PAT12345678",
            "user_type": "patient"
        }
        
        login_data = {
            "username": "PAT12345678",
            "password": "password123"
        }
        
        response = client.post("/auth/login", json=login_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["access_token"] == "mocked_jwt_token"
        assert data["token_type"] == "bearer"
        assert data["user_id"] == "PAT12345678"
        assert data["user_type"] == "patient"
        mock_authenticate.assert_called_once_with("PAT12345678", "password123")
    
    @patch('app.routes.auth_routes.auth_service.authenticate_user')
    def test_login_doctor_success(self, mock_authenticate, client):
        """Test successful doctor login"""
        mock_authenticate.return_value = {
            "token": "mocked_jwt_token",
            "user_id": "DOC87654321",
            "user_type": "doctor"
        }
        
        login_data = {
            "username": "DOC87654321",
            "password": "password123"
        }
        
        response = client.post("/auth/login", json=login_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["user_id"] == "DOC87654321"
        assert data["user_type"] == "doctor"
    
    @patch('app.routes.auth_routes.auth_service.authenticate_user')
    def test_login_with_email(self, mock_authenticate, client):
        """Test login with email address"""
        mock_authenticate.return_value = {
            "token": "mocked_jwt_token",
            "user_id": "PAT12345678",
            "user_type": "patient"
        }
        
        login_data = {
            "username": "patient@test.com",
            "password": "password123"
        }
        
        response = client.post("/auth/login", json=login_data)
        
        assert response.status_code == 200
        mock_authenticate.assert_called_once_with("patient@test.com", "password123")
    
    @patch('app.routes.auth_routes.auth_service.authenticate_user')
    def test_login_invalid_credentials(self, mock_authenticate, client):
        """Test login with invalid credentials"""
        mock_authenticate.side_effect = Exception("Invalid credentials")
        
        login_data = {
            "username": "invalid_user",
            "password": "wrong_password"
        }
        
        response = client.post("/auth/login", json=login_data)
        
        assert response.status_code == 401
        assert "Invalid credentials" in response.json()["detail"]
    
    @patch('app.routes.auth_routes.auth_service.generate_otp')
    @patch('app.routes.auth_routes.email_service.send_otp_email')
    def test_send_otp_success(self, mock_email, mock_generate, client):
        """Test successful OTP sending"""
        mock_generate.return_value = "123456"
        mock_email.return_value = None
        
        otp_data = {
            "email": "test@test.com"
        }
        
        response = client.post("/auth/send-otp", json=otp_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "OTP sent successfully" in data["message"]
        mock_generate.assert_called_once_with("test@test.com")
        mock_email.assert_called_once_with("test@test.com", "123456")
    
    @patch('app.routes.auth_routes.auth_service.verify_otp')
    def test_verify_otp_success(self, mock_verify, client):
        """Test successful OTP verification"""
        mock_verify.return_value = {"message": "Account verified successfully"}
        
        otp_data = {
            "email": "test@test.com",
            "otp": "123456"
        }
        
        response = client.post("/auth/verify-otp", json=otp_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "verified successfully" in data["message"]
        mock_verify.assert_called_once_with("test@test.com", "123456")
    
    @patch('app.routes.auth_routes.auth_service.verify_otp')
    def test_verify_otp_invalid(self, mock_verify, client):
        """Test OTP verification with invalid OTP"""
        mock_verify.side_effect = Exception("Invalid or expired OTP")
        
        otp_data = {
            "email": "test@test.com",
            "otp": "invalid"
        }
        
        response = client.post("/auth/verify-otp", json=otp_data)
        
        assert response.status_code == 400
        assert "Invalid or expired OTP" in response.json()["detail"]
    
    @patch('app.routes.auth_routes.auth_service.generate_password_reset_otp')
    @patch('app.routes.auth_routes.email_service.send_password_reset_email')
    def test_forgot_password_success(self, mock_email, mock_generate, client):
        """Test successful forgot password request"""
        mock_generate.return_value = "123456"
        mock_email.return_value = None
        
        forgot_data = {
            "email": "test@test.com"
        }
        
        response = client.post("/auth/forgot-password", json=forgot_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "Password reset OTP sent" in data["message"]
    
    @patch('app.routes.auth_routes.auth_service.reset_password')
    def test_reset_password_success(self, mock_reset, client):
        """Test successful password reset"""
        mock_reset.return_value = {"message": "Password reset successfully"}
        
        reset_data = {
            "email": "test@test.com",
            "otp": "123456",
            "new_password": "newpassword123"
        }
        
        response = client.post("/auth/reset-password", json=reset_data)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "reset successfully" in data["message"]
        mock_reset.assert_called_once_with("test@test.com", "123456", "newpassword123")
    
    @patch('app.routes.auth_routes.auth_service.validate_token')
    def test_validate_token_success(self, mock_validate, client):
        """Test successful token validation"""
        mock_validate.return_value = {"valid": True}
        
        response = client.post("/auth/validate-token?token=valid_token")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "Token is valid" in data["message"]
    
    def test_invalid_endpoint(self, client):
        """Test accessing invalid endpoint"""
        response = client.get("/auth/invalid")
        
        assert response.status_code == 404
