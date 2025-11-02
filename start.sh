#!/bin/bash

# Startup script for Spark Pipeline Benchmark Tool

echo "🚀 Starting Spark Pipeline Benchmark Tool"
echo "=========================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "⚠️  Virtual environment not found. Creating one..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Check for .env file
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found. Using default configuration."
    echo "   Copy .env.example to .env to customize settings."
    
    # Set default environment variables
    export SPARK_MASTER_HOST="local[*]"
    export MAX_DATASET_SIZE="120000"
    export NUM_STAGES="3"
    export DATASET_PATH="./train.csv"
else
    echo "✅ Loading environment variables from .env"
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check if dataset exists
if [ ! -f "$DATASET_PATH" ]; then
    echo "❌ Dataset not found at: $DATASET_PATH"
    echo "   Please ensure your dataset is available before running benchmarks."
fi

echo ""
echo "Configuration:"
echo "  - Spark Master: $SPARK_MASTER_HOST"
echo "  - Max Dataset Size: $MAX_DATASET_SIZE"
echo "  - Number of Stages: $NUM_STAGES"
echo "  - Dataset Path: $DATASET_PATH"
echo ""

# Start FastAPI server in background
echo "🌐 Starting FastAPI server on port 8000..."
python main.py &
API_PID=$!

# Wait for API to be ready
sleep 3

# Check if API is running
if curl -s http://localhost:8000/health > /dev/null; then
    echo "✅ FastAPI server is running (PID: $API_PID)"
else
    echo "❌ Failed to start FastAPI server"
    exit 1
fi

# Start Gradio UI
echo ""
echo "🎨 Starting Gradio UI on port 7860..."
python ui.py

# Cleanup on exit
trap "echo 'Shutting down servers...'; kill $API_PID 2>/dev/null" EXIT
