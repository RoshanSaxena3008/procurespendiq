#!/bin/bash
# Startup script for ProcureSpendIQ on Azure Static Web Apps

set -e

echo "=========================================="
echo "ProcureSpendIQ - Azure Deployment"
echo "=========================================="

echo "Step 1: Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Step 2: Starting Streamlit application..."
streamlit run app.py \
  --server.port 8000 \
  --server.address 0.0.0.0 \
  --logger.level info \
  --client.showErrorDetails true \
  --server.enableCORS true

echo "=========================================="
echo "Application started successfully!"
echo "=========================================="
