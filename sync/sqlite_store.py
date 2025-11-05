import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS local_files (
  rel_path TEXT PRIMARY KEY,
  size INTEGER,
  mtime_utc TEXT,
  md5 TEXT,
  qetag TEXT,
  ext TEXT,
  modified_by_device_id TEXT,
  deleted INTEGER DEFAULT 0,
  last_scanned_at_utc TEXT,
  last_synced_at_utc TEXT
);

CREATE TABLE IF NOT EXISTS server_index (
  rel_path TEXT PRIMARY KEY,
  size INTEGER,
  mtime_utc TEXT,
  md5 TEXT,
  qetag TEXT,
  ext TEXT,
  modified_by_device_id TEXT,
  deleted INTEGER,
  manifest_seq INTEGER
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT
);
"""


class SQLiteStore:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self):
        with self.conn:
            self.conn.executescript(SCHEMA)

    def upsert_local_file(self, record: Dict):
        cols = [
            "rel_path","size","mtime_utc","md5","qetag","ext",
            "modified_by_device_id","deleted","last_scanned_at_utc","last_synced_at_utc"
        ]
        values = [record.get(c) for c in cols]
        placeholders = ",".join(["?"] * len(cols))
        with self.conn:
            self.conn.execute(
                f"INSERT INTO local_files ({','.join(cols)}) VALUES ({placeholders})\n"
                f"ON CONFLICT(rel_path) DO UPDATE SET "
                f"size=excluded.size, mtime_utc=excluded.mtime_utc, md5=excluded.md5, qetag=excluded.qetag, ext=excluded.ext, "
                f"modified_by_device_id=excluded.modified_by_device_id, deleted=excluded.deleted, last_scanned_at_utc=excluded.last_scanned_at_utc",
                values,
            )

    def replace_server_index(self, rows: Iterable[Dict], manifest_seq: int):
        with self.conn:
            self.conn.execute("DELETE FROM server_index")
            for r in rows:
                self.conn.execute(
                    """
                    INSERT INTO server_index
                    (rel_path,size,mtime_utc,md5,qetag,ext,modified_by_device_id,deleted,manifest_seq)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        r.get("rel_path"), r.get("size"), r.get("mtime_utc"), r.get("md5"), r.get("qetag"),
                        r.get("ext"), r.get("modified_by_device_id"), r.get("deleted", 0), manifest_seq,
                    ),
                )

    def get_setting(self, key: str) -> Optional[str]:
        cur = self.conn.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    def set_setting(self, key: str, value: str):
        with self.conn:
            self.conn.execute(
                "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass 