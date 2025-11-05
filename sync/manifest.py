from dataclasses import dataclass, field, asdict
from typing import Dict, Any
from datetime import datetime, timezone


MANIFEST_KEY = "__sync/manifest.json"


@dataclass
class ManifestEntry:
    rel_path: str
    size: int
    mtime_utc: str
    md5: str
    qetag: str
    ext: str
    modified_by_device_id: str
    deleted: int = 0


@dataclass
class Manifest:
    version: int
    manifest_seq: int
    generated_at_utc: str
    generator_device_id: str
    files: Dict[str, ManifestEntry] = field(default_factory=dict)

    @staticmethod
    def empty(device_id: str) -> "Manifest":
        now = datetime.now(timezone.utc).isoformat()
        return Manifest(version=1, manifest_seq=0, generated_at_utc=now, generator_device_id=device_id, files={})

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["files"] = {k: asdict(v) for k, v in self.files.items()}
        return d

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Manifest":
        files = {k: ManifestEntry(**v) for k, v in d.get("files", {}).items()}
        return Manifest(
            version=d.get("version", 1),
            manifest_seq=d.get("manifest_seq", 0),
            generated_at_utc=d.get("generated_at_utc"),
            generator_device_id=d.get("generator_device_id", ""),
            files=files,
        ) 