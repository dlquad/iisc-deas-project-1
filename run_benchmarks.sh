#!/bin/bash

set -e

CONFIG_FILE="${1:-benchmark_configs.json}"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Config file not found: $CONFIG_FILE"
    echo "Usage: ./run_benchmarks.sh [config_file]"
    exit 1
fi

echo "📋 Loading configurations from: $CONFIG_FILE"
echo ""

CONFIGS=$(cat "$CONFIG_FILE")
NUM_CONFIGS=$(echo "$CONFIGS" | jq '. | length')

echo "Found $NUM_CONFIGS configurations to benchmark"
echo ""

for i in $(seq 0 $(($NUM_CONFIGS - 1))); do
    CONFIG=$(echo "$CONFIGS" | jq ".[$i]")
    
    NAME=$(echo "$CONFIG" | jq -r '.name')
    NUM_WORKERS=$(echo "$CONFIG" | jq -r '.num_workers')
    MEM_PER_WORKER=$(echo "$CONFIG" | jq -r '.mem_per_worker')
    CORES_PER_WORKER=$(echo "$CONFIG" | jq -r '.cores_per_worker')
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔧 Configuration $(($i + 1))/$NUM_CONFIGS: $NAME"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "   Workers: $NUM_WORKERS"
    echo "   Memory per Worker: ${MEM_PER_WORKER}GB"
    echo "   Cores per Worker: $CORES_PER_WORKER"
    echo ""
    
    docker compose down --remove-orphans 2>/dev/null || true
    
    echo "🔨 Generating docker-compose.yaml..."
    python generate_compose.py "$NUM_WORKERS" "$MEM_PER_WORKER" "$CORES_PER_WORKER"
    
    echo "🚀 Starting Docker Compose deployment..."
    docker compose up -d > /dev/null
    
    echo "⏳ Waiting for services to be ready..."
    sleep 15
    
    echo "📊 Running benchmark..."
    echo "$CONFIG" | jq -s '.' > /tmp/current_config.json
    python benchmark.py /tmp/current_config.json
    
    echo ""
    echo "🛑 Stopping Docker Compose deployment..."
    docker compose down
    
    echo ""
    echo "✅ Configuration $NAME completed"
    echo ""
    echo "════════════════════════════════════════════════════════════════════════════════"
    echo "════════════════════════════════════════════════════════════════════════════════"
    echo "════════════════════════════════════════════════════════════════════════════════"
    echo ""
    
    if [ $i -lt $(($NUM_CONFIGS - 1)) ]; then
        echo "⏸  Waiting 5 seconds before next configuration..."
        sleep 5
        echo ""
    fi
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎉 All benchmarks completed successfully!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
