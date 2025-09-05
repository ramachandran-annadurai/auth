# Patient & Doctor Auth Service

Authentication microservice for the patient and doctor management system.

## Features

- **Dual User Types**: Patient and Doctor registration/login
- **Configurable Collections**: Collection names configurable via `config.json`
- **Default Collections**: `patients_v2` and `doctor_v2` (configurable)
- **Unique ID Generation**: PAT12345678 (patients) and DOC12345678 (doctors)
- **Unified Authentication**: Same endpoints for both user types
- **OTP-First Registration**: Account created ONLY after OTP verification
- **Pending User Management**: Temporary storage for unverified registrations
- **Role-based JWT Tokens**: Tokens include user type for authorization
- **Password Management**: Reset functionality for both user types
- **Secure Authentication**: Bcrypt password hashing and JWT tokens
- **Session Management**: JWT-based sessions with database tracking
- **Admin Dashboard**: Monitor pending registrations and system status

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Setup environment:**
   ```bash
   # Copy environment template
   cp env.txt .env
   
   # Edit .env with your actual credentials
   nano .env
   ```

3. **Start the service:**
   ```bash
   python start.py
   ```

The service will run on `http://localhost:5001`

## 🌐 HTML Testing Interface

### **Web-based Testing Pages:**
- **Main Interface**: `http://localhost:5001/test` - Login & Signup forms
- **Session Management**: `http://localhost:5001/static/sessions.html` - Manage active sessions  
- **OTP Verification**: `http://localhost:5001/static/otp.html` - Email verification & password reset
- **Admin Dashboard**: `http://localhost:5001/static/admin.html` - Monitor pending registrations

### **Features of HTML Interface:**
- ✅ **Patient & Doctor Registration** with role-specific fields
- ✅ **Multi-login Support** (Patient ID, Doctor ID, username, email)
- ✅ **Session Management** with real-time status
- ✅ **OTP Verification** and **Password Reset** workflows
- ✅ **Real-time Service Status** indicator
- ✅ **Beautiful Responsive UI** with gradient design
- ✅ **Token Storage** in localStorage for session persistence

## API Endpoints

- `POST /auth/register` - Register patient or doctor
- `POST /auth/generate-otp` - Generate OTP for email verification
- `POST /auth/verify-otp` - Verify OTP and activate account
- `POST /auth/login` - Login with Patient ID, Doctor ID, username, or email
- `POST /auth/logout` - Logout current session
- `POST /auth/logout-all` - Logout all sessions
- `GET /auth/sessions` - View active sessions
- `POST /auth/forgot-password` - Send password reset OTP
- `POST /auth/reset-password` - Reset password with OTP
- `POST /auth/validate-token` - Validate JWT token
- `GET /health` - Health check
- `GET /test` - HTML testing interface
- `GET /static/*` - Static files (HTML, CSS, JS)

### Admin Endpoints:
- `GET /admin/pending-users` - View pending registrations
- `DELETE /admin/pending-users/{user_id}` - Delete pending registration
- `POST /admin/pending-users/{user_id}/resend-otp` - Resend OTP for pending user
- `GET /admin/collections-status` - Get database collections status

## 🔧 Configuration

### Collection Names (config.json)
You can customize collection names in `config.json`:

```json
{
  "database": {
    "database_name": "patients_db",
    "collections": {
      "patients": "patients_v2",
      "doctors": "doctor_v2",
      "pending_users": "pending_users",
      "otp_codes": "otp_codes",
      "user_sessions": "user_sessions"
    }
  }
}
```

### Environment Variables (.env)
```env
MONGO_URI=mongodb://localhost:27017
JWT_SECRET=auto-generated-secure-key
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-gmail-app-password
```

## Registration Examples

### Patient Registration:
```json
POST /auth/register
{
  "username": "john_patient",
  "email": "john@example.com",
  "mobile": "1234567890",
  "password": "securepass123",
  "first_name": "John",
  "last_name": "Doe",
  "user_type": "patient",
  "is_pregnant": false
}
```

### Doctor Registration:
```json
POST /auth/register
{
  "username": "dr_smith",
  "email": "dr.smith@hospital.com",
  "mobile": "0987654321", 
  "password": "securepass123",
  "first_name": "Dr. Sarah",
  "last_name": "Smith",
  "user_type": "doctor",
  "specialization": "Cardiology"
}
```

## Login Examples

### Login with Patient ID:
```json
POST /auth/login
{
  "username": "PAT12345678",
  "password": "securepass123"
}
```

### Login with Doctor ID:
```json
POST /auth/login
{
  "username": "DOC87654321", 
  "password": "securepass123"
}
```

### Login with Email (works for both):
```json
POST /auth/login
{
  "username": "john@example.com",
  "password": "securepass123"
}
```

## API Documentation

Access interactive API docs at: `http://localhost:5001/docs`

## Postman Collection

### Import & Test:
```bash
# Import these files into Postman:
- Patient_Doctor_Auth_API.postman_collection.json
- Patient_Doctor_Auth_Environment.postman_environment.json
```

### Collection Features:
- **30+ API requests** covering all authentication endpoints
- **Patient & Doctor workflows** with complete registration/login flows
- **Automated test scripts** with response validation
- **Environment variables** for easy configuration
- **Error scenario testing** for robust validation
- **JWT token management** with automatic saving

### Quick Test:
1. Import collection into Postman
2. Select "Patient Doctor Auth Environment"
3. Start auth service: `python start.py`
4. Run "Health Check" request to verify connection
5. Test complete patient/doctor registration flows

See `POSTMAN_GUIDE.md` for detailed usage instructions.

## Testing

### Run Tests:
```bash
# Run all tests
python run_tests.py all

# Run specific test types
python run_tests.py unit         # Unit tests only
python run_tests.py integration  # Integration tests only
python run_tests.py coverage     # With coverage report

# Run specific test file
python run_tests.py specific test_auth_service.py

# Install test dependencies (if needed)
python run_tests.py install
```

### Test Coverage:
The test suite includes:
- **Unit Tests**: Auth service, email service, security utilities, configuration
- **Integration Tests**: Complete authentication flows, API endpoints
- **Route Tests**: FastAPI endpoint testing with mocked services
- **Security Tests**: Password hashing, JWT token validation
- **Configuration Tests**: Environment and JSON configuration loading

## Configuration Management

### Sensitive Data (.env file)
- `MONGO_URI` - Database connection string
- `JWT_SECRET` - JWT signing secret
- `SMTP_USER` - Email username
- `SMTP_PASSWORD` - Email password

### Non-sensitive Settings (config.json)
- Service settings (port, host, debug)
- JWT algorithm and expiration
- Email server settings
- Database name

### Quick Setup
```bash
cp env.txt .env          # Copy template
nano .env                # Edit with your credentials
rm env.txt              # Delete template for security
python start.py         # Start service
```

## Deployment

See `DEPLOYMENT.md` for detailed deployment instructions and `DEVELOPMENT_GUIDE.md` for comprehensive development documentation.

**Quick deployment:**
1. `cp env.txt .env` - Copy environment template
2. Edit `.env` with your actual credentials
3. `rm env.txt` - Delete template file for security
4. `python start.py` - Start service

This service can be deployed on any platform that supports Python:
- AWS Lambda, EC2, ECS
- Render, Railway, Heroku
- Google Cloud Run, App Engine
- Traditional VPS/dedicated servers

**For detailed architecture, development workflow, and next steps, see:**
- `DEVELOPMENT_GUIDE.md` - Comprehensive development documentation
- `DEPLOYMENT.md` - Platform-specific deployment instructions
- `POSTMAN_GUIDE.md` - API testing with Postman
