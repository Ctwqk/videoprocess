# Streaming Voice Chat Bot

链路：
- 音频输入 -> `faster-whisper` 转文字
- 文字 -> OpenAI 兼容 LLM 流式回复
- 文本增量 -> XTTS WebSocket 流式语音输出

## 1) 服务器（Docker）

```bash
cd /home/taiwei/VideoProcess/voice_chat_bot
docker compose up -d --build
curl http://localhost:8090/health
```

当前已配置容器直连 XTTS：
- `TTS_WS_URL=ws://xtts-api:8000/v1/tts/ws`

## 2) 本机实时对话（麦克风 -> 机器人 -> 输出设备）

本机安装依赖：

```bash
python3 -m pip install websockets sounddevice soundfile numpy
```

列设备：

```bash
python3 mic_stream_client.py --list-devices
```

运行：

```bash
python3 mic_stream_client.py \
  --ws ws://localhost:8090/ws/voice \
  --input-device 你的输入设备ID \
  --output-device 你的输出设备ID
```

## 3) 设备桥接（监听一个设备并转发到另一个设备）

```bash
python3 audio_bridge.py --list-devices
python3 audio_bridge.py \
  --input-device 源设备ID \
  --output-device 目标设备ID \
  --sample-rate 48000 \
  --channels 1
```

## 4) BlackHole 网络聊天路由（你这个场景）

目标：
- 让机器人听到“系统/会议软件输出”
- 让机器人回复注入到“会议软件麦克风输入”

推荐配置：
1. 在 macOS 音频 MIDI 设置里创建 `Multi-Output Device`（Mac 扬声器 + BlackHole）。
2. 把会议软件“扬声器/输出”设为这个 `Multi-Output Device`。
3. 运行机器人客户端时：
   - `--input-device` 选 BlackHole（监听对方声音）
   - `--output-device` 也可选 BlackHole（把机器人声音注入虚拟麦克风链路）
4. 把会议软件“麦克风/输入”设为 BlackHole。

示例：

```bash
python3 mic_stream_client.py \
  --ws ws://localhost:8090/ws/voice \
  --input-device 5 \
  --output-device 5 \
  --stt-language zh \
  --tts-language zh-cn
```

备注：脚本默认会在每轮回复后清空采集缓冲，降低 BlackHole 自激回声风险。

## 5) 客户端 Docker（可选，主要 Linux）

已提供 `Dockerfile.client` + compose `audio-client`（profile: `client-linux`）。

```bash
docker compose --profile client-linux up -d --build audio-client
```

说明：
- 该模式依赖 `/dev/snd`，主要适用于 Linux。
- macOS 上 Docker Desktop 对宿主音频设备访问受限，建议直接运行本机 Python 脚本（上面的方式）。

## 6) 远端设备流式演示（不依赖本机声卡库）

在任意设备上（Windows/macOS/Linux）安装：

```bash
python3 -m pip install websockets
```

运行演示客户端，把音频分片流式推到 Linux：

```bash
python3 remote_stream_demo.py \
  --ws ws://<linux-ip>:8090/ws/voice \
  --input ./input.wav \
  --out-dir ./demo_out \
  --chunk-bytes 4096 \
  --chunk-delay-ms 30 \
  --stt-language zh \
  --tts-language zh-cn
```

输出：
- 控制台实时打印 `stt` 与 `llm_delta`（文本流）
- `demo_out/reply_0001.wav`, `reply_0002.wav`, ...（语音流分段）

## 7) Web UI (set input/output devices)

After service starts, open:

- `http://<server-ip>:8090/`

Use the page to:
- Refresh/select input device (mic)
- Refresh/select output device (speaker, browser support required)
- Connect session
- Start Talking / Stop & Commit

Notes:
- Output device selection depends on browser support for `HTMLMediaElement.setSinkId` (works in Chromium-based browsers, limited on Safari).
- Browser asks for microphone permission before listing device labels.

## Auto conversation mode on Web UI

- Click `Start Auto Talk` once.
- The page listens continuously.
- It auto-commits each utterance after silence (no manual submit).
- While assistant is replying, capture is paused, then auto-resumes listening.
