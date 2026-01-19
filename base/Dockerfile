FROM python:3.11-slim
WORKDIR /app

# Install ffmpeg for audio extraction/transcoding.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements from the repo (context must be repo root)
COPY base/requirements.txt /app/.base-requirements.txt
RUN pip install --no-cache-dir -r /app/.base-requirements.txt

# Copy full repo context after deps for better layering
COPY . .
