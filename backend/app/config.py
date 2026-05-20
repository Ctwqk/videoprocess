from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    deploy_mode: str = "shared"

    # Database
    database_url: str = "postgresql+asyncpg://vp:vp_secret@localhost:5435/videoprocess"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Storage
    storage_backend: str = "local"  # "local" or "minio"
    storage_local_root: str = "/tmp/vp_storage"

    # MinIO
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "videoprocess"
    minio_secure: bool = False

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    cors_origins: list[str] = ["*"]
    exo_watchdog_url: str = "http://localhost:8000"
    youtube_manager_url: str = "http://localhost:8899"
    platform_browser_manager_url: str = "http://localhost:8898"
    x_platform_browser_manager_url: str = ""
    bilibili_platform_browser_manager_url: str = ""
    xiaohongshu_platform_browser_manager_url: str = ""

    # Shared retrieval services
    embedding_gateway_url: str = "http://localhost:8080"
    qdrant_url: str = "http://localhost:6333"
    material_qdrant_collection: str = "videoprocess_material_clips"
    material_lighthouse_url: str = ""
    material_univtg_url: str = ""
    vision_embedding_url: str = ""
    smart_trim_vlm_url: str = ""
    smart_trim_default_worker_type: str = "vision"

    # AutoFlow AI assistance. Disabled by default so tests and constrained
    # workers remain deterministic unless explicitly opted in.
    autoflow_ai_enabled: bool = False
    autoflow_llm_gateway_url: str = "http://127.0.0.1:8000"
    autoflow_llm_source: str = "videoprocess"
    autoflow_llm_profile: str = "generic_chat"
    autoflow_embedding_url: str = ""
    autoflow_qdrant_url: str = "http://127.0.0.1:6333"
    autoflow_ai_timeout_seconds: float = 8.0

    # ChannelOps Agent
    channel_agent_alert_slack_webhook_url: str = ""
    channel_agent_alert_email_to: str = ""
    channel_agent_runner_poll_seconds: float = 5.0
    channel_agent_scheduler_poll_seconds: float = 60.0
    channel_agent_retention_queue_days: int = 30
    channel_agent_retention_audit_days: int = 90
    channel_agent_retention_feedback_days: int = 365

    # Policy Decision Service
    pds_enabled: bool = False
    pds_base_url: str = "http://pds:8080"
    pds_client_id: str = "videoprocess-channel-agent"
    pds_timeout_seconds: float = 0.5

    # Risk event Kafka
    risk_kafka_brokers: str = "redpanda:9092"
    risk_vp_actions_topic: str = "vp.actor.actions.v1"
    risk_outbox_batch_size: int = 100
    risk_outbox_poll_seconds: float = 1.0
    risk_outbox_max_backoff_seconds: float = 60.0
    risk_outbox_metrics_port: int = 9101

    # MiniMax image generation. Default endpoint/model were checked against
    # the MiniMax CN docs for the image_generation API on 2026-05-18.
    minimax_api_key: str = ""
    minimax_image_generation_url: str = "https://api.minimaxi.com/v1/image_generation"
    minimax_model: str = "image-01"
    minimax_timeout_seconds: float = 30.0
    minimax_retry_count: int = 1
    minimax_max_qps: float = 2.0

    video_schedule_default_state: str = "OPEN"

    # Video worker features
    video_use_gpu: bool = False
    video_use_videotoolbox: bool = False

    model_config = {"env_prefix": "", "case_sensitive": False}


settings = Settings()
