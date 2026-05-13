FROM nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    ffmpeg \
    libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --extra-index-url https://download.pytorch.org/whl/cu121 \
    torch==2.3.1+cu121 torchaudio==2.3.1+cu121 && \
    python3 -m pip install -r /app/requirements.txt

COPY app /app/app
RUN mkdir -p /app/output /app/speaker-cache /app/voicesource /root/.local/share/tts

EXPOSE 8010

CMD ["python3", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8010"]
