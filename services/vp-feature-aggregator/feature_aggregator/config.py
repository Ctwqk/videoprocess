from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    http_host: str = "0.0.0.0"
    http_port: int = 8080
    kafka_brokers: str = "redpanda:9092"
    kafka_group_id: str = "vp-feature-aggregator"
    vp_actions_topic: str = "vp.actor.actions.v1"
    pds_decisions_topic: str = "pds.decisions.v1"
    dead_letter_topic: str = "risk.events.dlq.v1"
    enable_consumer: bool = False
    database_url: str = "postgresql://vp:vp_secret@localhost:5435/videoprocess"
    redis_url: str = "redis://localhost:6380/2"
    dedupe_ttl_seconds: int = 604800

    model_config = {"env_prefix": "AGG_", "case_sensitive": False}


settings = Settings()
