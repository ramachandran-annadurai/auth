# 🚀 Deployment Guide

## Quick Deployment Steps

### 1. **Clone Repository**
```bash
git clone <your-repo-url>
cd patient-auth-service
```

### 2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

### 3. **Setup Environment Variables**
```bash
# Copy deployment template to .env
cp env.txt .env

# Edit .env with your actual credentials
nano .env  # or use any text editor

# Update these values in .env:
# MONGO_URI=your-actual-mongodb-connection-string
# JWT_SECRET=your-super-secret-random-string
# SMTP_USER=your-actual-email@gmail.com
# SMTP_PASSWORD=your-actual-gmail-app-password
```

### 4. **Delete Template File (IMPORTANT)**
```bash
# Delete the template file for security
rm env.txt
```

### 5. **Start Service**
```bash
# For development
python start.py

# For production (with gunicorn)
pip install gunicorn
gunicorn -w 4 -k uvicorn.workers.UvicornWorker app.main:app --bind 0.0.0.0:5001
```

## Platform-Specific Deployment

### **Render**
1. Connect your GitHub repository
2. Set build command: `pip install -r requirements.txt`
3. Set start command: `python start.py`
4. Add environment variables in Render dashboard:
   - `MONGO_URI`
   - `JWT_SECRET`
   - `SMTP_USER`
   - `SMTP_PASSWORD`

### **Railway**
1. Connect GitHub repository
2. Railway auto-detects Python app
3. Add environment variables in Railway dashboard
4. Deploy automatically

### **AWS Lambda**
1. Install dependencies: `pip install -r requirements.txt -t .`
2. Add Mangum to requirements: `pip install mangum`
3. Create deployment package
4. Set environment variables in Lambda console

### **Traditional VPS**
1. Clone repository
2. Follow steps 1-5 above
3. Setup systemd service or use screen/tmux
4. Configure nginx reverse proxy if needed

## Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `MONGO_URI` | MongoDB connection string | `mongodb://localhost:27017` |
| `JWT_SECRET` | JWT signing secret (MUST be secure) | `super-secret-random-string-12345` |
| `SMTP_USER` | Gmail username for sending emails | `your-email@gmail.com` |
| `SMTP_PASSWORD` | Gmail app password | `your-gmail-app-password` |

## Security Checklist

- ✅ **Delete env.txt** after copying to .env
- ✅ **Never commit .env** to version control
- ✅ **Use strong JWT_SECRET** (random 32+ characters)
- ✅ **Use Gmail App Password** (not regular password)
- ✅ **Secure MongoDB URI** (use authentication)
- ✅ **HTTPS in production** (SSL/TLS certificates)

## Testing Deployment

```bash
# Test health endpoint
curl http://your-domain:5001/health

# Test API documentation
curl http://your-domain:5001/docs

# Test auth endpoint
curl -X POST http://your-domain:5001/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"test","email":"test@example.com","password":"test123","mobile":"1234567890","first_name":"Test","last_name":"User"}'
```

## Troubleshooting

### Common Issues:
1. **Database connection failed** - Check MONGO_URI
2. **Email not sending** - Verify SMTP_USER and SMTP_PASSWORD
3. **JWT errors** - Ensure JWT_SECRET is set
4. **Port conflicts** - Check if port 5001 is available

### Logs:
- Check application logs for detailed error messages
- Monitor database connectivity
- Verify email service configuration

## Production Considerations

1. **Use production MongoDB** (MongoDB Atlas recommended)
2. **Set strong JWT_SECRET** (generate random 32+ character string)
3. **Enable HTTPS** (SSL/TLS certificates)
4. **Monitor logs** and set up alerting
5. **Regular backups** of database
6. **Rate limiting** for API endpoints
7. **Health checks** and monitoring
