FROM nvidia/cuda:12.3.2-cudnn9-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install -U pip && \
    python3 -m pip install faster-whisper

WORKDIR /work
CMD ["python3"]
