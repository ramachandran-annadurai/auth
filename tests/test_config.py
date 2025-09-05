import pytest
import os
import json
from unittest.mock import patch, mock_open
from app.utils.config import Config

class TestConfig:
    
    @patch.dict(os.environ, {
        'MONGO_URI': 'mongodb://test:27017',
        'JWT_SECRET': 'test_jwt_secret',
        'SMTP_USER': 'test@gmail.com',
        'SMTP_PASSWORD': 'test_password'
    })
    @patch('builtins.open', mock_open(read_data='{"database": {"database_name": "test_db"}}'))
    def test_config_with_env_vars(self):
        """Test configuration loading with environment variables"""
        config = Config()
        
        # Test environment variables
        assert config.mongo_uri == 'mongodb://test:27017'
        assert config.jwt_secret == 'test_jwt_secret'
        assert config.smtp_user == 'test@gmail.com'
        assert config.smtp_password == 'test_password'
        
        # Test JSON config
        assert config.database_name == 'test_db'
    
    @patch.dict(os.environ, {}, clear=True)
    @patch('builtins.open', mock_open(read_data='{"service": {"port": 8080}}'))
    @patch('app.utils.config.set_key')
    def test_config_with_defaults(self, mock_set_key):
        """Test configuration with default values"""
        config = Config()
        
        # Test default values
        assert config.mongo_uri == 'mongodb://localhost:27017'
        # JWT secret will be auto-generated, so just check it exists
        assert len(config.jwt_secret) > 10
        assert config.smtp_user is None
        assert config.smtp_password is None
        assert config.port == 8080
    
    @patch('builtins.open', side_effect=FileNotFoundError)
    @patch('app.utils.config.set_key')
    def test_config_without_json_file(self, mock_set_key, mock_open):
        """Test configuration when JSON file doesn't exist"""
        config = Config()
        
        # Should use default configuration
        assert config.database_name == 'patients_db'
        assert config.jwt_algorithm == 'HS256'
        assert config.jwt_expire_minutes == 30
        assert config.smtp_host == 'smtp.gmail.com'
        assert config.smtp_port == 587
    
    @patch('builtins.open', mock_open(read_data='{"jwt": {"algorithm": "HS512", "expire_minutes": 60}}'))
    def test_config_json_override(self):
        """Test JSON configuration overriding defaults"""
        config = Config()
        
        assert config.jwt_algorithm == 'HS512'
        assert config.jwt_expire_minutes == 60
    
    @patch('builtins.open', mock_open(read_data='{"email": {"smtp_host": "smtp.custom.com", "smtp_port": 465}}'))
    def test_config_email_settings(self):
        """Test email configuration settings"""
        config = Config()
        
        assert config.smtp_host == 'smtp.custom.com'
        assert config.smtp_port == 465
    
    @patch.dict(os.environ, {'PORT': '9000'})
    @patch('builtins.open', mock_open(read_data='{"service": {"port": 8080}}'))
    def test_port_environment_override(self):
        """Test that PORT environment variable overrides JSON config"""
        config = Config()
        
        # Environment variable should take precedence
        assert config.port == 9000
    
    @patch.dict(os.environ, {'DEBUG': 'true'})
    def test_debug_environment_variable(self):
        """Test debug mode from environment variable"""
        config = Config()
        
        assert config.debug == True
    
    @patch.dict(os.environ, {'DEBUG': 'false'})
    def test_debug_false_environment_variable(self):
        """Test debug mode false from environment variable"""
        config = Config()
        
        assert config.debug == False
    
    @patch('builtins.open', mock_open(read_data='{"service": {"debug": true}}'))
    def test_debug_from_json(self):
        """Test debug mode from JSON configuration"""
        config = Config()
        
        assert config.debug == True
    
    def test_get_method(self):
        """Test the get method for nested configuration access"""
        config_data = {
            "database": {"database_name": "test_db"},
            "jwt": {"algorithm": "HS256"}
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(config_data))):
            config = Config()
            
            assert config.get('database.database_name') == 'test_db'
            assert config.get('jwt.algorithm') == 'HS256'
            assert config.get('nonexistent.key') is None
            assert config.get('nonexistent.key', 'default') == 'default'
    
    def test_get_method_with_invalid_path(self):
        """Test get method with invalid key path"""
        config_data = {"database": {"name": "test"}}
        
        with patch('builtins.open', mock_open(read_data=json.dumps(config_data))):
            config = Config()
            
            assert config.get('database.invalid') is None
            assert config.get('invalid.path', 'default_value') == 'default_value'
    
    @patch('builtins.open', mock_open(read_data='{"service": {"host": "127.0.0.1"}}'))
    def test_service_configuration(self):
        """Test service-specific configuration"""
        config = Config()
        
        assert config.host == '127.0.0.1'
        assert config.debug == False  # Default value
    
    def test_all_properties_accessible(self):
        """Test that all configuration properties are accessible"""
        config = Config()
        
        # Database properties
        assert hasattr(config, 'mongo_uri')
        assert hasattr(config, 'database_name')
        
        # JWT properties
        assert hasattr(config, 'jwt_secret')
        assert hasattr(config, 'jwt_algorithm')
        assert hasattr(config, 'jwt_expire_minutes')
        
        # Email properties
        assert hasattr(config, 'smtp_host')
        assert hasattr(config, 'smtp_port')
        assert hasattr(config, 'smtp_user')
        assert hasattr(config, 'smtp_password')
        
        # Service properties
        assert hasattr(config, 'port')
        assert hasattr(config, 'host')
        assert hasattr(config, 'debug')
