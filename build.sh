#!/bin/bash

# Build script for Patient Auth Service
echo "Building Patient Auth Service..."

# Install dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Run tests (optional)
echo "Running tests..."
python -m pytest tests/ -v

echo "Build completed successfully!"
