FROM apache/airflow:2.7.3-python3.10

USER root

# Install system dependencies for Prophet
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    libffi-dev \
    libssl-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r /requirements.txt

# Install cmdstanpy and cmdstan
RUN pip install --no-cache-dir cmdstanpy==1.2.0 && \
    python -c "import cmdstanpy; cmdstanpy.install_cmdstan()" || echo "cmdstan install completed"

# Set environment variables
ENV CMDSTAN=/home/airflow/.cmdstan