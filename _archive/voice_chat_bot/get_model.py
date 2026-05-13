import logging
import os
import requests

logger = logging.getLogger("voice-chat-bot")

MODEL_FILE = os.getenv("EXO_MODEL_FILE", "/home/taiwei/Constructure/infra/exo/model.txt")


def _parse_model_line(line: str) -> str:
    line = (line or "").strip()
    if not line:
        return ""
    if line.endswith(")") and "(" in line:
        return line.rsplit("(", 1)[-1].rstrip(")").strip()
    return line


def _read_model_from_txt(model_file: str = MODEL_FILE) -> str:
    try:
        with open(model_file, "r", encoding="utf-8") as fh:
            candidates = [_parse_model_line(line) for line in fh]
    except OSError as exc:
        logger.warning("failed to read exo model file %s: %s", model_file, exc)
        return ""

    env_model = os.getenv("LLM_MODEL", "").strip()
    candidates = [candidate for candidate in candidates if candidate]
    if env_model and env_model in candidates:
        logger.info("resolved LLM model from model.txt via env match: %s", env_model)
        return env_model
    if candidates:
        logger.info("resolved LLM model from model.txt: %s", candidates[0])
        return candidates[0]
    return ""


def get_current_model(exo_url="http://192.168.20.2:52415"):
    txt_model = _read_model_from_txt()
    if txt_model:
        return txt_model
    try:
        state = requests.get(f"{exo_url}/state").json()
        for iid, inst in state.get("instances", {}).items():
            inner = inst.get("MlxRingInstance", {})
            model_id = inner.get("shardAssignments", {}).get("modelId")
            if model_id:
                logger.info("resolved LLM model from exo state: %s", model_id)
                return model_id
    except Exception as exc:
        logger.warning("failed to query exo state for model: %s", exc)
    fallback = os.getenv("LLM_MODEL", "mlx-community/GLM-4.7-Flash-5bit")
    logger.info("using fallback LLM model: %s", fallback)
    return fallback
