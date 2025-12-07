#!/bin/bash

# Define venv directory
VENV_DIR="venv"

# Check if venv exists
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv $VENV_DIR
else
    echo "Virtual environment found."
fi

# echo "setting proxy..."
# export HTTPS_PROXY="http://10.0.0.5:7800"

# Install dependencies
if [ -f "requirements.txt" ]; then
    echo "Installing dependencies..."
    ./$VENV_DIR/bin/pip install -r requirements.txt
else
    echo "requirements.txt not found, skipping installation."
fi

# Run the application
echo "Starting application..."
./$VENV_DIR/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8000