# Use Python 3.11 with PySpark
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Verify Java installation and set up proper paths
RUN java -version && \
    echo "Java installation verified" && \
    ls -la /usr/lib/jvm/ && \
    update-alternatives --display java && \
    ln -sf /usr/lib/jvm/default-java /usr/lib/jvm/java-11-openjdk-amd64 || true

# Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz \
    && tar -xzf spark-3.5.6-bin-hadoop3.tgz \
    && mv spark-3.5.6-bin-hadoop3 /opt/spark \
    && rm spark-3.5.6-bin-hadoop3.tgz

# Fix Spark's Java path configuration
RUN sed -i 's|/usr/lib/jvm/java-11-openjdk-amd64|/usr/lib/jvm/default-java|g' /opt/spark/conf/spark-env.sh.template || true

# Set working directory
WORKDIR /app

# Copy poetry files and README
COPY pyproject.toml poetry.lock README.md ./

# Install poetry
RUN pip install poetry

# Configure poetry to not create virtual environment
RUN poetry config virtualenvs.create false

# Copy application code first
COPY src/ ./src/
COPY tests/ ./tests/

# Install dependencies and the project
RUN poetry install

# Create directories for output
RUN mkdir -p /app/output /app/checkpoint /app/spark-warehouse /app/spark-temp

# Expose ports
EXPOSE 4040 7077

# Set default command
CMD ["poetry", "run", "tunetracker", "stream"] 