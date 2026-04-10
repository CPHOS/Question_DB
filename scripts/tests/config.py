from pathlib import Path
import os


ROOT_DIR = Path(__file__).resolve().parents[2]
TMP_DIR = ROOT_DIR / "tmp"
SAMPLES_DIR = TMP_DIR / "samples"
DOWNLOADS_DIR = TMP_DIR / "downloads"
API_LOG_PATH = TMP_DIR / "qb_api_e2e.log"
EXPORT_PATH = TMP_DIR / "qb_e2e_internal.jsonl"
QUALITY_PATH = TMP_DIR / "qb_e2e_quality.json"
DB_BACKUP_PATH = DOWNLOADS_DIR / "qb_e2e_backup.sql"
REPORT_PATH = TMP_DIR / "qb_e2e_report.md"
INVALID_PAPER_UPLOAD_PATH = SAMPLES_DIR / "paper_invalid_upload.bin"
REAL_TEST_ZIP_PATH = ROOT_DIR / "scripts" / "test.zip"
REAL_TEST2_ZIP_PATH = ROOT_DIR / "scripts" / "test2.zip"


def _load_dotenv() -> dict[str, str]:
    """Parse the root .env file into a dict (no shell expansion)."""
    env_file = ROOT_DIR / ".env"
    vals: dict[str, str] = {}
    if not env_file.exists():
        return vals
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        vals[key.strip()] = value.strip()
    return vals


_dotenv = _load_dotenv()


def _cfg(key: str, default: str) -> str:
    """Resolve config: environment variable > .env file > default."""
    return os.environ.get(key, _dotenv.get(key, default))


POSTGRES_USER = _cfg("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = _cfg("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = _cfg("POSTGRES_DB", "qb")
POSTGRES_MAJOR = _cfg("QB_POSTGRES_MAJOR", "16")

CONTAINER_NAME = _cfg("CONTAINER_NAME", "qb-postgres-e2e")
POSTGRES_IMAGE = _cfg("POSTGRES_IMAGE", f"postgres:{POSTGRES_MAJOR}")
POSTGRES_PORT = _cfg("POSTGRES_PORT", "55433")
API_PORT = _cfg("QB_BIND_PORT", "8080")
DB_URL = f"postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{POSTGRES_PORT}/{POSTGRES_DB}"
