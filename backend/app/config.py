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

    video_schedule_default_state: str = "OPEN"

    # Video worker features
    video_use_gpu: bool = False
    video_use_videotoolbox: bool = False

    model_config = {"env_prefix": "", "case_sensitive": False}


settings = Settings()
