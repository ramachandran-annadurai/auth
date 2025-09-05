# 📮 **Postman Collection Guide - Patient & Doctor Auth API**

## 🚀 **Quick Setup**

### **1. Import Collection & Environment**
```bash
1. Open Postman
2. Click "Import" button
3. Import these files:
   - Patient_Doctor_Auth_API.postman_collection.json
   - Patient_Doctor_Auth_Environment.postman_environment.json
4. Select "Patient Doctor Auth Environment" from environment dropdown
```

### **2. Start Auth Service**
```bash
# Start the auth service
python start.py

# Service runs on http://localhost:5001
```

## 📋 **Collection Structure**

### **🏥 Health & Info**
- **Health Check** - Service health status
- **Service Info** - Basic service information  
- **API Documentation** - Interactive API docs

### **👤 Patient Authentication**
- **Register Patient** - Create new patient account (generates PAT12345678)
- **Register Pregnant Patient** - Patient with pregnancy tracking
- **Send OTP to Patient** - Email verification code
- **Verify Patient OTP** - Activate patient account
- **Login Patient with ID** - Login using PAT12345678
- **Login Patient with Email** - Login using email address
- **Login Patient with Username** - Login using username

### **👨‍⚕️ Doctor Authentication**
- **Register Doctor** - Create new doctor account (generates DOC87654321)
- **Register Specialist Doctor** - Doctor with specialization
- **Send OTP to Doctor** - Email verification for doctors
- **Verify Doctor OTP** - Activate doctor account
- **Login Doctor with ID** - Login using DOC87654321
- **Login Doctor with Email** - Login using email address

### **🔐 Password Management**
- **Forgot Password - Patient** - Request password reset for patient
- **Forgot Password - Doctor** - Request password reset for doctor
- **Reset Password - Patient** - Reset patient password with OTP
- **Reset Password - Doctor** - Reset doctor password with OTP

### **🎫 Token Validation**
- **Validate Patient Token** - Verify patient JWT token
- **Validate Doctor Token** - Verify doctor JWT token

### **❌ Error Scenarios**
- **Register with Invalid User Type** - Test validation
- **Register with Invalid Email** - Test email validation
- **Login with Wrong Password** - Test authentication
- **Login Non-existent User** - Test user validation
- **Verify Invalid OTP** - Test OTP validation

## 🔄 **Complete Testing Workflow**

### **Patient Registration Flow:**
```
1. Register Patient → 2. Send OTP → 3. Verify OTP → 4. Login → 5. Use Token
```

### **Doctor Registration Flow:**
```
1. Register Doctor → 2. Send OTP → 3. Verify OTP → 4. Login → 5. Use Token
```

## 🧪 **Automated Tests**

### **Built-in Test Scripts:**
- ✅ **Status Code Validation** - Ensures correct HTTP responses
- ✅ **Response Structure** - Validates JSON response format
- ✅ **Token Extraction** - Automatically saves JWT tokens
- ✅ **User Type Validation** - Confirms patient vs doctor
- ✅ **ID Format Validation** - Checks PAT/DOC ID patterns
- ✅ **Error Message Validation** - Verifies error responses

### **Environment Variables:**
- `base_url` - Service URL (http://localhost:5001)
- `auth_token` - Patient JWT token (auto-saved)
- `doctor_token` - Doctor JWT token (auto-saved)
- `patient_id` - Sample patient ID (PAT12345678)
- `doctor_id` - Sample doctor ID (DOC87654321)
- `patient_email` - Test patient email
- `doctor_email` - Test doctor email

## 📊 **Test Results**

### **Expected Responses:**

#### **Successful Patient Registration:**
```json
{
  "message": "User registered successfully. Please verify OTP.",
  "success": true
}
```

#### **Successful Login:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGci...",
  "token_type": "bearer",
  "user_id": "PAT12345678",
  "user_type": "patient"
}
```

#### **Error Response:**
```json
{
  "detail": "Invalid credentials"
}
```

## 🎯 **Testing Scenarios**

### **1. Happy Path Testing:**
- Register → Verify → Login → Use Token
- Test both patient and doctor flows
- Test different login methods (ID, email, username)

### **2. Error Path Testing:**
- Invalid user types
- Wrong passwords
- Invalid OTP codes
- Non-existent users

### **3. Security Testing:**
- Token validation
- Password reset flow
- OTP expiration
- Input validation

## 🔧 **Customization**

### **Update Base URL:**
```json
// In environment variables
"base_url": "https://your-production-url.com"
```

### **Add Custom Tests:**
```javascript
// In request Tests tab
pm.test("Custom validation", function () {
    var jsonData = pm.response.json();
    pm.expect(jsonData.custom_field).to.exist;
});
```

### **Environment Switching:**
- **Development**: `http://localhost:5001`
- **Staging**: `https://staging.yourapi.com`
- **Production**: `https://api.yourapp.com`

## 🚀 **Running Tests**

### **Individual Requests:**
1. Select request
2. Click "Send"
3. View response and test results

### **Collection Runner:**
1. Click "Runner" button
2. Select collection
3. Choose environment
4. Click "Run Collection"
5. View detailed test results

### **Automated Testing:**
1. Use Newman CLI for CI/CD
2. Run collection programmatically
3. Generate test reports

## 📈 **Monitoring & Reporting**

### **Test Reports:**
- Response times
- Success/failure rates
- Error patterns
- Performance metrics

### **Collection Sharing:**
- Export collection for team sharing
- Version control with Git
- Collaborative testing

## 🎉 **Ready to Test!**

**Your Postman collection includes:**
- ✅ **30+ API requests** covering all endpoints
- ✅ **Automated test scripts** with validations
- ✅ **Environment variables** for easy configuration
- ✅ **Complete workflows** for patient and doctor authentication
- ✅ **Error scenario testing** for robust validation
- ✅ **Token management** with automatic saving

**Import the collection and start testing your Patient & Doctor Auth API!** 🚀
