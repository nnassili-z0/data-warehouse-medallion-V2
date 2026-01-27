#!/bin/bash
# Test runner script for the Medallion Data Warehouse project

set -e

echo "Running unit tests for Medallion Data Warehouse..."

# Install test dependencies if not already installed
pip install -r requirements.txt

# Run pytest with verbose output
python -m pytest tests/ -v --tb=short

echo "All tests completed successfully!"