from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    PROJECT_NAME: str = "Stock API"
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_PASSWORD: Optional[str] = None
    DATABASE_URL: str = "sqlite:///./stock_api.db"

    class Config:
        env_file = ".env"

settings = Settings()
