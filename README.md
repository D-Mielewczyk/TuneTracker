# TuneTracker

A simple real-time streaming analytics project for music play events, using [kafka-python](https://github.com/dpkp/kafka-python) and PySpark.

## Authors

| Name     | Surname    | Student Index |
|----------|------------|---------------|
| Dawid    | Mielewczyk | 189637        |
| Wojciech | Szamocki   | 188909        |

## 🎯 What It Does

TuneTracker simulates a music streaming service that:

- **Produces** random music play events (user_id, track_id, genre, timestamp)  
- **Streams** these events through Kafka  
- **Aggregates** play counts by genre in 1-minute windows using PySpark  
- **Outputs** results to a single CSV file (`results.csv`)  

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
├── pyproject.toml                # Poetry configuration
└── README.md                     # This file
````

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+** (managed by Poetry)
- **Apache Kafka** running locally or via Docker Compose
- **Java 11+** (required for PySpark)

### Installation

1. **Clone and install dependencies:**

   ```bash
   git clone <repository-url>
   cd TuneTracker
   poetry install
   ```

2. **Start Kafka locally (via Docker Compose):**

   ```bash
   docker-compose up -d
   ```

   This will start **Zookeeper + Kafka + Spark**.

### Usage

#### 1. Produce Music Events

```bash
poetry run tunetracker produce \
    --bootstrap-servers localhost:9092 \
    --topic music-plays \
    --rate 2 \
    --total 100
```

#### 2. Stream and Aggregate

```bash
poetry run tunetracker stream \
    --bootstrap-servers localhost:9092 \
    --input-topic music-plays \
    --output-path ./output \
    --checkpoint-location ./checkpoint
```

#### 3. Run Complete Demo (producer + streaming together)

```bash
poetry run tunetracker demo \
    --bootstrap-servers localhost:9092 \
    --topic music-plays \
    --duration 60
```

## 📊 Output

The streaming job produces aggregated results showing play counts by genre in 1-minute windows.
They are written into a single **CSV file** inside your chosen `--output-path` (default: `./demo_output/results.csv`):

```csv
window_start,window_end,genre,count
2025-09-07 21:00:00,2025-09-07 21:01:00,pop,15
2025-09-07 21:00:00,2025-09-07 21:01:00,rock,8
2025-09-07 21:01:00,2025-09-07 21:02:00,jazz,3
```

Each new batch is **appended** to the same file.

## 🏗️ Architecture

### Components

- **`main.py`**: CLI orchestration using Typer (commands: produce, stream, demo)
- **`producer.py`**: Kafka producer logic using [kafka-python](https://github.com/dpkp/kafka-python)
- **`streaming.py`**: PySpark streaming + CSV aggregation logic

### Data Flow

```bash
Random Events → Kafka → PySpark Streaming → Aggregated Results (CSV)
```

### Technologies

- **[kafka-python](https://github.com/dpkp/kafka-python)**: Kafka client for Python
- **PySpark**: Distributed computing and streaming
- **Typer**: Modern CLI framework
- **Poetry**: Dependency management

## 🎓 Learning Goals

This project demonstrates:

- **Real-time Streaming**: End-to-end streaming pipeline from event production to analytics
- **Kafka Integration**: Producing and consuming messages
- **PySpark**: Windowed aggregations and batch processing
- **Music Analytics**: Genre-based play count analysis
- **Modern Python**: Poetry, Typer, type hints, logging

## 🔧 Development

### Project Setup

```bash
# Install dependencies
poetry install

# Run tests
poetry run pytest

# Format code
poetry run black src/ tests/

# Lint code
poetry run ruff check src/ tests/
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
