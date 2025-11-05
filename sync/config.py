import json
import os
import platform
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional


APP_DIR_NAME = "NTXT_SYNC"


@dataclass
class AppConfig:
    device_id: Optional[str] = None
    local_dir: Optional[str] = None

    qiniu_access_key: Optional[str] = None
    qiniu_secret_key: Optional[str] = None
    qiniu_bucket: Optional[str] = None
    qiniu_domain: Optional[str] = None  # with scheme, e.g. https://xxx.com
    qiniu_region: Optional[str] = None
    qiniu_subdir: Optional[str] = None

    scan_interval_minutes: int = 5
    force_upload_ignore_lock: bool = False


def app_data_dir() -> Path:
    try:
        system = platform.system()
        if system == "Windows":
            base = os.getenv("LOCALAPPDATA") or os.getenv("APPDATA") or str(Path.home() / "AppData" / "Local")
            return Path(base) / APP_DIR_NAME
        elif system == "Darwin":
            return Path.home() / "Library" / "Application Support" / APP_DIR_NAME
        else:
            return Path(os.getenv("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))) / APP_DIR_NAME
    except Exception:
        return Path.cwd() / APP_DIR_NAME


def default_config_path() -> str:
    p = app_data_dir()
    p.mkdir(parents=True, exist_ok=True)
    return str(p / "config.json")


def load_config(path: str) -> AppConfig:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return AppConfig(**data)
    except Exception:
        pass
    return AppConfig()


def save_config(path: str, cfg: AppConfig) -> None:
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(asdict(cfg), f, ensure_ascii=False, indent=2)
    except Exception as e:
        # Best-effort save; surface errors to caller if needed
        raise 