# AudioChattingBot

A multi-service audio and media workflow platform that combines orchestration APIs, browser automation, transcription, TTS, and voice-chat utilities.

At the top level, this repository brings together several related services:

- a FastAPI orchestration backend
- a React frontend
- browser / platform automation helpers
- YouTube management utilities
- Faster-Whisper transcription tools
- streaming voice-chat experiments

## Highlights

- FastAPI backend for jobs, assets, artifacts, pipelines, and node-type orchestration
- React + TypeScript frontend for interacting with the platform
- PostgreSQL, Redis, and MinIO infrastructure wired through Docker Compose
- Separate browser and YouTube management services
- Faster-Whisper transcription utilities and XTTS / voice-chat experiments
- Worker-oriented architecture for media-processing flows

## Tech Stack

- Python
- FastAPI
- SQLAlchemy / asyncpg
- Redis
- PostgreSQL
- MinIO
- React
- TypeScript
- Vite
- Docker Compose

## Repository Layout

```text
backend/                 # Main API and orchestration engine
frontend/                # React UI
PlatformBrowserManager/  # Browser automation / profile management service
YouTubeManager/          # YouTube-specific service
FasterWhisper/           # Transcription experiments
TextToAudio/             # XTTS-based text-to-speech service
voice_chat_bot/          # Streaming voice chat pipeline
deploy/                  # Deployment notes and topology docs
```

## Quick Start

### Prerequisites

- Docker
- Docker Compose

### Start the stack

```bash
docker compose up --build
```

After startup, the main exposed services include:

- Frontend: `http://localhost:3001`
- API: `http://localhost:8080`
- YouTube Manager: `http://localhost:8899`
- Platform Browser Manager: `http://localhost:8898`
- MinIO Console: `http://localhost:9001`

## Notes

- The repository is a monorepo of related services rather than a single small app.
- Some subdirectories already contain their own focused README files; the root README is meant to orient you at the platform level.
