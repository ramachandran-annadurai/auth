#!/usr/bin/env python3
"""
Test Runner Script for Patient Auth Service
Provides easy commands to run different types of tests
"""

import subprocess
import sys
import os

def run_command(command):
    """Run a shell command and return the result"""
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed: {command}")
        print(f"Error: {e.stderr}")
        return False

def main():
    if len(sys.argv) < 2:
        print("🧪 Patient Auth Service Test Runner")
        print("\nUsage:")
        print("  python run_tests.py all              # Run all tests")
        print("  python run_tests.py unit             # Run unit tests only")
        print("  python run_tests.py integration      # Run integration tests only")
        print("  python run_tests.py coverage         # Run tests with coverage report")
        print("  python run_tests.py specific <file>  # Run specific test file")
        print("  python run_tests.py install          # Install test dependencies")
        print("\nExamples:")
        print("  python run_tests.py all")
        print("  python run_tests.py specific test_auth_service.py")
        print("  python run_tests.py coverage")
        return
    
    command_type = sys.argv[1]
    
    # Change to the correct directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    if command_type == "install":
        print("📦 Installing test dependencies...")
        success1 = run_command("pip install --upgrade pip")
        success2 = run_command("pip install pytest pytest-asyncio httpx pytest-cov")
        success3 = run_command("pip install -r requirements.txt")
        if success1 and success2 and success3:
            print("✅ Test dependencies installed successfully!")
        else:
            print("❌ Some installations failed. Try running: python setup_tests.py")
        return
    
    elif command_type == "all":
        print("🧪 Running all tests...")
        success = run_command("python -m pytest tests/ -v")
        
    elif command_type == "unit":
        print("🔧 Running unit tests...")
        success = run_command("python -m pytest tests/test_auth_service.py tests/test_email_service.py tests/test_security.py tests/test_config.py -v")
        
    elif command_type == "integration":
        print("🔗 Running integration tests...")
        success = run_command("python -m pytest tests/test_auth_routes.py tests/test_integration.py -v")
        
    elif command_type == "coverage":
        print("📊 Running tests with coverage report...")
        success = run_command("python -m pytest tests/ --cov=app --cov-report=html --cov-report=term")
        if success:
            print("\n📄 Coverage report generated in htmlcov/index.html")
        
    elif command_type == "specific" and len(sys.argv) >= 3:
        test_file = sys.argv[2]
        print(f"🎯 Running specific test: {test_file}")
        success = run_command(f"python -m pytest tests/{test_file} -v")
        
    else:
        print("❌ Invalid command. Use 'python run_tests.py' for help.")
        return
    
    if success:
        print("✅ Tests completed successfully!")
    else:
        print("❌ Some tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
