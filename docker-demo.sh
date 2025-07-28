#!/bin/bash

echo "🚀 Starting TuneTracker Demo with Docker..."

# Build and start all services
echo "📦 Building and starting services..."
docker-compose up -d

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 30

# Run the demo
echo "🎵 Running TuneTracker demo..."
docker-compose run --rm tunetracker poetry run tunetracker demo --duration 30

echo "✅ Demo completed! Check the ./output directory for results."
echo "🛑 Stopping services..."
docker-compose down 