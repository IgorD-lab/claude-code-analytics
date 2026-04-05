FROM python:3.12-slim

WORKDIR /app

# Install dependencies before copying source so this layer is cached
# independently of code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project.
COPY . .

# Generate synthetic telemetry data and load it into SQLite at build time.
# The resulting database is baked into the image so both services can read it
# immediately on startup with no extra initialisation step.
RUN mkdir -p data/raw data/processed \
    && python3 data/generate_fake_data.py \
        --num-users 100 --num-sessions 5000 --days 60 \
        --output-dir data/raw \
    && python src/data_ingestion.py

EXPOSE 8501 8000
