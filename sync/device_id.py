import hashlib
import json
import os
import platform
import subprocess
import uuid
from typing import Optional

from .config import AppConfig


def _get_machine_fingerprint() -> str:
    parts = []
    try:
        parts.append(platform.node())
    except Exception:
        pass
    try:
        if platform.system() == "Windows":
            try:
                # Disk serial
                out = subprocess.check_output(
                    ["wmic", "diskdrive", "get", "SerialNumber"],
                    stderr=subprocess.DEVNULL,
                    text=True,
                    timeout=3,
                )
                parts.append(out.strip())
            except Exception:
                pass
            try:
                out = subprocess.check_output(
                    ["wmic", "baseboard", "get", "Product"],
                    stderr=subprocess.DEVNULL,
                    text=True,
                    timeout=3,
                )
                parts.append(out.strip())
            except Exception:
                pass
        else:
            # Non-Windows: fall back to uname
            parts.append(" ".join(platform.uname()))
    except Exception:
        pass
    return "|".join(p for p in parts if p)


def generate_device_id() -> str:
    fp = _get_machine_fingerprint()
    if not fp:
        return str(uuid.uuid4())
    digest = hashlib.sha256(fp.encode("utf-8")).hexdigest()
    return f"dev-{digest[:16]}"


def ensure_device_id(cfg: AppConfig) -> None:
    if not cfg.device_id:
        cfg.device_id = generate_device_id() 