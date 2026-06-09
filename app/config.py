import json
import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    NAMESPACE: str = os.getenv("NAMESPACE", "default")
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    DB_PATH: str = os.getenv("DB_PATH", "/data/cnpg-ui.db")
    PORT: int = int(os.getenv("PORT", "8080"))
    S3_BUCKET: str = os.getenv("S3_BUCKET", "")
    DEFAULT_CLUSTER: str = os.getenv("DEFAULT_CLUSTER", "postgres")
    AWS_CREDENTIALS_SECRET: str = os.getenv("AWS_CREDENTIALS_SECRET", "aws-credentials")
    STORAGE_CLASS: str = os.getenv("STORAGE_CLASS", "")
    NODE_SELECTOR: dict = json.loads(os.getenv("NODE_SELECTOR", "{}"))
    TOLERATIONS: list = json.loads(os.getenv("TOLERATIONS", "[]"))


settings = Settings()
