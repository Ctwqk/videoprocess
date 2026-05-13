# XTTS v2 + FastAPI GPU Docker Service

在当前目录提供一个基于 `XTTS v2` 的 HTTP TTS 服务，适合被其它服务调用。

## 1. 前置条件

- Linux 主机
- 安装 Docker + Docker Compose
- NVIDIA 驱动 + NVIDIA Container Toolkit

## 2. 启动

```bash
docker compose build
docker compose up -d
```

首次启动会下载 XTTS v2 模型到 `./models`（已做 volume 持久化）。

说明：本服务在容器环境下通过 `COQUI_TOS_AGREED=1` 预先同意 Coqui XTTS 的许可确认，避免首次拉取模型时出现交互式阻塞。

## 3. 健康检查

```bash
curl http://localhost:8000/health
```

## 4. TTS 接口

`POST /v1/tts` (multipart/form-data)

字段：
- `text`：要合成的文本
- `speaker_wav`：可选，参考人声 wav 文件（用于克隆音色）
- `language`：语言代码，默认 `en`，中文可用 `zh-cn`
- `output_filename`：可选，输出文件名

如果不传 `speaker_wav`，服务端必须配置 `DEFAULT_SPEAKER_WAV` 指向一个默认参考音频文件。

示例（传入参考人声）：

```bash
curl -X POST http://localhost:8000/v1/tts \
  -F 'text=你好，这是一个XTTS v2测试。' \
  -F 'language=zh-cn' \
  -F 'speaker_wav=@./sample.wav'
```

示例（不传参考人声，使用服务端默认音色）：

```bash
curl -X POST http://localhost:8000/v1/tts \
  -F 'text=你好，这是一个XTTS v2测试。' \
  -F 'language=zh-cn'
```

返回：

```json
{
  "message": "ok",
  "file": "tts_xxx.wav",
  "download_url": "/v1/files/tts_xxx.wav"
}
```

`POST /v1/tts/stream` (multipart/form-data)

- 直接返回 `audio/wav` 分块流，适合实时播放链路（如 BlackHole 输入）。
- 参数与 `/v1/tts` 一致（`text`、`speaker_wav`、`language`）。

示例（流式输出到文件）：

```bash
curl -X POST http://localhost:8000/v1/tts/stream \
  -F 'text=你好，这是流式输出测试。' \
  -F 'language=zh-cn' \
  -F 'speaker_wav=@./sample.wav' \
  --output stream.wav
```

`WS /v1/tts/ws?language=zh-cn`

- WebSocket 流输入/流输出（输入文本片段，输出二进制 WAV 音频帧）。
- 默认使用 `DEFAULT_SPEAKER_WAV`，也可通过 query 参数指定服务端文件路径：
  `/v1/tts/ws?language=zh-cn&speaker_wav_path=/path/to/ref.wav`
- 控制消息：
  - 发送普通文本：追加到缓冲区，遇到句末符号自动触发合成并回传音频帧
  - 发送 `__flush__`：立即合成剩余缓冲文本
  - 发送 `__close__`：合成剩余文本并关闭连接

`websocat` 示例：

```bash
websocat "ws://localhost:8000/v1/tts/ws?language=zh-cn"
```

连接后可直接输入文本分段，输入 `__flush__` 强制输出，输入 `__close__` 结束。

## 5. 下载音频

```bash
curl -L "http://localhost:8000/v1/files/tts_xxx.wav" -o out.wav
```

## 6. 常用运维

查看日志：

```bash
docker compose logs -f xtts-api
```

停止：

```bash
docker compose down
```

如果你修改了依赖（例如 `requirements.txt`），请强制重建镜像：

```bash
docker compose build --no-cache
docker compose up -d --force-recreate
```
