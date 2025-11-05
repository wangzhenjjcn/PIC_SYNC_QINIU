from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

LOCK_KEY = "__sync/lock.json"


@dataclass
class LeaseLock:
    owner_device_id: str
    locked_at_utc: str
    expires_at_utc: str
    manifest_seq_when_locked: int
    nonce: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def new(owner_device_id: str, manifest_seq: int, ttl_minutes: int = 15, nonce: str = "") -> "LeaseLock":
        now = datetime.now(timezone.utc)
        exp = now + timedelta(minutes=ttl_minutes)
        return LeaseLock(
            owner_device_id=owner_device_id,
            locked_at_utc=now.isoformat(),
            expires_at_utc=exp.isoformat(),
            manifest_seq_when_locked=manifest_seq,
            nonce=nonce,
        )

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "LeaseLock":
        return LeaseLock(
            owner_device_id=d.get("owner_device_id", ""),
            locked_at_utc=d.get("locked_at_utc", ""),
            expires_at_utc=d.get("expires_at_utc", ""),
            manifest_seq_when_locked=d.get("manifest_seq_when_locked", 0),
            nonce=d.get("nonce", ""),
        )

    def expires_dt(self) -> datetime:
        try:
            dt = datetime.fromisoformat(self.expires_at_utc)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return datetime.now(timezone.utc) - timedelta(days=1)

    def is_expired(self) -> bool:
        try:
            exp = self.expires_dt()
            return datetime.now(timezone.utc) >= exp
        except Exception:
            return True

    def is_expired_with_grace(self, grace_minutes: int) -> bool:
        try:
            exp = self.expires_dt() + timedelta(minutes=grace_minutes)
            return datetime.now(timezone.utc) >= exp
        except Exception:
            return True 