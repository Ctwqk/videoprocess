# VideoProcess Service

VideoProcess provides media processing, AutoFlow planning, and channel operations.
See the [current architecture contracts](../../current-architecture.md) and the
[repository overview](../../../README.md) for deployment and service ownership.

## Current components

| Component | Location | Responsibility |
| --- | --- | --- |
| Python API and media workers | `backend/` | AutoFlow, orchestration, transcription, and media processing |
| Go runtime | `cmd/`, `internal/` | Production ChannelOps, HTTP API, and FFmpeg workers |
| Web application | `frontend/` | React workflow interface |
| Feature aggregator | `services/vp-feature-aggregator/` | Actor features for policy decisions |
| Host-native TTS | `TextToAudio/` | Text-to-audio deployment via `deploy/macos/deploy_tts_services.sh` |

Platform upload services are maintained in the companion
`constructure-platform-upload` repository. Retired standalone Whisper and
voice-chat experiments, duplicate archived sources, and the old UI ZIP are
available through Git history rather than the current checkout.
