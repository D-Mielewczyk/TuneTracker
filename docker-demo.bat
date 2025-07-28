@echo off
echo 🚀 Starting TuneTracker Demo with Docker...

REM Build and start all services
echo 📦 Building and starting services...
docker-compose up -d

REM Wait for Kafka to be ready
echo ⏳ Waiting for Kafka to be ready...
timeout /t 30 /nobreak >nul

REM Run the demo
echo 🎵 Running TuneTracker demo...
docker-compose run --rm tunetracker poetry run tunetracker demo --duration 30

echo ✅ Demo completed! Check the ./output directory for results.
echo 🛑 Stopping services...
docker-compose down 