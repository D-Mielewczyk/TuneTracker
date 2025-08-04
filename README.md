# TuneTracker

A simple real-time streaming analytics project for music play events, using [kafka-python](https://github.com/dpkp/kafka-python) and PySpark Structured Streaming.

## 🎯 What It Does

TuneTracker simulates a music streaming service that:

- **Produces** random music play events (user_id, track_id, genre, timestamp)
- **Streams** these events through Kafka
- **Aggregates** play counts by genre in 1-minute windows using PySpark
- **Outputs** results to Parquet format

## 📁 Project Structure

```bash
TuneTracker/
├── src/
│   └── tunetracker/              # Main package
│       ├── __init__.py           # Package metadata
│       ├── main.py               # CLI orchestration
│       ├── producer.py           # Kafka producer logic
│       └── streaming.py          # PySpark streaming logic
├── tests/                        # Unit and integration tests
├── pyproject.toml               # Poetry configuration
└── README.md                    # This file
```

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+** (managed by Poetry)
- **Apache Kafka** running locally (for testing)
- **Java 8+** (required for PySpark)

### Installation

1. **Clone and install dependencies:**
  
   ```powershell
   git clone <repository-url>
   cd TuneTracker
   poetry install
   ```

2. **Start Kafka locally:**

   ```powershell
   # Download and start Kafka
   # Create topic: music-plays
   kafka-topics --create --topic music-plays --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

### Usage

#### 1. Produce Music Events

```powershell
poetry run tunetracker produce `
    --bootstrap-servers localhost:9092 `
    --topic music-plays `
    --rate 2 `
    --total 100
```

#### 2. Stream and Aggregate

```powershell
poetry run tunetracker stream `
    --bootstrap-servers localhost:9092 `
    --input-topic music-plays `
    --output-path ./output `
    --checkpoint-location ./checkpoint
```

#### 3. Run Complete Demo

```powershell
poetry run tunetracker demo `
    --bootstrap-servers localhost:9092 `
    --topic music-plays `
    --duration 60
```

#### 4. Consolidate Parquet Files

```powershell
poetry run tunetracker consolidate `
    --output-path ./output `
    --target-file-size-gb 1.0
```

## 📊 Output

The streaming job produces aggregated results showing play counts by genre in 1-minute windows:

```parquet
window_start,window_end,genre,count
2024-01-01 10:00:00,2024-01-01 10:01:00,pop,15
2024-01-01 10:00:00,2024-01-01 10:01:00,rock,8
2024-01-01 10:01:00,2024-01-01 10:02:00,pop,12
2024-01-01 10:01:00,2024-01-01 10:02:00,jazz,3
```

## 🔄 File Consolidation

The system automatically consolidates parquet files at the end of streaming sessions to optimize storage and query performance:

- **Intelligent Sizing**: Aims for ~1GB files by default (configurable)
- **Existing File Consideration**: Consolidates both new and existing parquet files
- **Backup Creation**: Creates timestamped backups before consolidation
- **Size Verification**: Reports actual file sizes and partition counts
- **Manual Consolidation**: Use the `consolidate` command for maintenance

### Consolidation Features

- **Automatic**: Runs before demo completion (always enabled)
- **Manual**: Available via CLI for maintenance tasks
- **Configurable**: Target file size can be adjusted
- **Safe**: Creates backups before consolidation
- **Informative**: Provides detailed logging of the process

## 🧪 Testing

Run the test suite:

```powershell
poetry run pytest
```

## 🏗️ Architecture

### Components

- **`main.py`**: CLI orchestration using Typer - provides commands for produce, stream, demo, and consolidate
- **`producer.py`**: Module containing Kafka producer logic using [kafka-python](https://github.com/dpkp/kafka-python)
- **`streaming.py`**: Module containing PySpark Structured Streaming logic for aggregations and file consolidation

### Data Flow

```bash
Random Events → Kafka → PySpark Streaming → Aggregated Results
     ↓              ↓           ↓                ↓
main.py        kafka-python  main.py         CSV/Delta
```

### Technologies

- **[kafka-python](https://github.com/dpkp/kafka-python)**: Simple Kafka client for Python
- **PySpark**: Distributed computing and streaming
- **Typer**: Modern CLI framework
- **Poetry**: Dependency management

## 🎓 Learning Goals

This project demonstrates:

- **Real-time Streaming**: End-to-end streaming pipeline from event production to analytics
- **Kafka Integration**: Using [kafka-python](https://github.com/dpkp/kafka-python) for simple, reliable messaging
- **PySpark Structured Streaming**: Windowed aggregations and real-time processing
- **Music Analytics**: Genre-based play count analysis (common in music streaming services)
- **Modern Python**: Poetry, Typer, type hints, and proper packaging

## 🔧 Development

### Project Setup

```powershell
# Install dependencies
poetry install

# Run tests
poetry run pytest

# Format code
poetry run black src/ tests/

# Lint code
poetry run ruff check src/ tests/
```

### Adding New Features

1. **New Producer Logic**: Add functions to `src/tunetracker/producer.py`
2. **New Streaming Logic**: Add functions to `src/tunetracker/streaming.py`
3. **New CLI Commands**: Add commands to `src/tunetracker/main.py` (the only CLI entry point)
4. **Tests**: Add corresponding tests in `tests/`

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
