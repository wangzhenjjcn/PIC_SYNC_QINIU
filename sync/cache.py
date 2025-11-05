import json
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

from .config import app_data_dir


# manifest cache will be per-profile file: manifest_cache_<profile>.json

def _cache_file(profile_key: str) -> Path:
    return app_data_dir() / f"manifest_cache_{profile_key}.json"


def load_manifest_cache(profile_key: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        path = _cache_file(profile_key)
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            return data.get("manifest"), data.get("etag")
    except Exception:
        pass
    return None, None


def save_manifest_cache(profile_key: str, manifest: Dict[str, Any], etag: Optional[str] = None) -> None:
    try:
        path = _cache_file(profile_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"manifest": manifest, "etag": etag}
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass 