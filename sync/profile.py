import hashlib
import json
from pathlib import Path
from typing import Optional, Dict, Any

from .config import app_data_dir


STATE_FILE = app_data_dir() / "profile_state.json"


def compute_profile_key(bucket: Optional[str], domain: Optional[str], local_dir: Optional[str], subdir: Optional[str]) -> str:
    sub = (subdir or '').strip().strip('/\\')
    raw = f"bucket={bucket or ''}|domain={domain or ''}|local={Path(local_dir or '').resolve()}|subdir={sub}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def load_last_profile_key() -> Optional[str]:
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            return data.get("last_profile_key")
    except Exception:
        pass
    return None


def save_last_profile_key(key: str) -> None:
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {"last_profile_key": key}
        STATE_FILE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass 