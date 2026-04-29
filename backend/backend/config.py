# backend/config.py
from pathlib import Path
import os
import logging
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DEV_ENV = BASE_DIR / ".env"
CI_ENV = BASE_DIR / ".env.ci"
PROD_ENV = BASE_DIR / ".env.prod"

# .env(dev) 기본 로드
if DEV_ENV.exists():
    load_dotenv(dotenv_path=DEV_ENV)

# APP_ENV 확인 (없으면 dev)
APP_ENV = os.getenv("APP_ENV", "dev").lower()

# 환경별 override
if APP_ENV == "ci" and CI_ENV.exists():
    load_dotenv(dotenv_path=CI_ENV, override=True)
elif APP_ENV == "prod" and PROD_ENV.exists():
    load_dotenv(dotenv_path=PROD_ENV, override=True)

# 환경 변수
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEFAULT_REGION = os.getenv("DEFAULT_REGION", "KOREA")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "16"))

# 필수 검증
_missing = [k for k in ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY") if not globals().get(k)]
if _missing:
    raise RuntimeError(f"Missing required env keys: {', '.join(_missing)} (APP_ENV={APP_ENV})")

# 로그 설정 및 알림
logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
logging.info(f"[CONFIG] Loaded environment: {APP_ENV}")
