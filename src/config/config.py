from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):
    source_host: str
    source_port: int
    source_database: str
    source_username: str
    source_password: str

    target_host: str
    target_port: int
    target_database: str
    target_username: str
    target_password: str

    class Config:
        case_sensitive = False
        env_file = Path(__file__).resolve().parent.parent.parent / ".env"


settings = Settings()
