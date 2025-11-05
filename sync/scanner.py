import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List


def _utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def file_md5(path: str, chunk_size: int = 1024 * 1024) -> str:
    m = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            m.update(chunk)
    return m.hexdigest()


def pseudo_qetag(path: str) -> str:
    # Placeholder: for MVP we reuse md5; later replace with qetag algorithm
    return file_md5(path)


def scan_directory(root: str) -> Iterable[Dict]:
    root_path = Path(root)
    for dirpath, dirnames, filenames in os.walk(root_path):
        # prune ignored directories globally
        dirnames[:] = [d for d in dirnames if d not in {"__sync", ".stfolder"}]
        # at top-level, only traverse allowed subfolders
        if Path(dirpath).resolve() == root_path.resolve():
            allowed = {"index.files", "new", "images", "assets", "android"}
            dirnames[:] = [d for d in dirnames if d.lower() in allowed]
        for name in filenames:
            # file-level ignores
            lname = name.lower()
            if lname == ".stfolder" or lname == ".htaccess":
                continue
            ext_lower = Path(name).suffix.lower()
            if ext_lower in {".exe", ".txt", ".ini", ".xls", ".xlsx", ".doc", ".docx", ".ppt", ".pptx", ".ink", ".apk", ".zip", ".pdf", ".tmp"}:
                continue

            rel = str(Path(dirpath, name).relative_to(root_path)).replace("\\", "/")
            if rel.startswith("__sync/") or "/__sync/" in rel or "/.stfolder/" in rel:
                continue
            fp = str(Path(dirpath, name))
            try:
                st = os.stat(fp)
                size = st.st_size
                mtime = _utc_iso(st.st_mtime)
                ext = Path(name).suffix.lower()
                md5 = file_md5(fp)
                qetag = pseudo_qetag(fp)
                yield {
                    "rel_path": rel,
                    "size": size,
                    "mtime_utc": mtime,
                    "md5": md5,
                    "qetag": qetag,
                    "ext": ext,
                }
            except Exception:
                continue 