import json
import os
from dataclasses import asdict
from typing import Dict, Optional, Tuple

from qiniu import Auth, put_file, BucketManager

from .manifest import MANIFEST_KEY, Manifest
from .lock import LOCK_KEY


class QiniuClient:
    def __init__(self, access_key: str, secret_key: str, bucket: str, domain: Optional[str] = None, region: Optional[str] = None):
        self.ak = access_key
        self.sk = secret_key
        self.bucket = bucket
        self.domain = domain
        self.region = region
        self.auth = Auth(self.ak, self.sk)
        self.bm = BucketManager(self.auth)

    # Manifest
    def download_manifest(self) -> Tuple[Optional[Manifest], Optional[str]]:
        try:
            # Use private download url if domain provided; fallback to bucket fetch (not direct supported here)
            base_url = f"{self.domain}/{MANIFEST_KEY}" if self.domain else None
            if base_url:
                private_url = self.auth.private_download_url(base_url, expires=60)
                import requests
                r = requests.get(private_url, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    # Get ETag header if present
                    etag = r.headers.get("ETag")
                    return Manifest.from_dict(data), etag
                elif r.status_code == 404:
                    return None, None
            # Fallback: try stat; if not exists, return None
            ret, info = self.bm.stat(self.bucket, MANIFEST_KEY)
            if info.status_code == 612:  # no such file
                return None, None
            # If exists, but domain not provided, we cannot read without download url
            return None, None
        except Exception:
            return None, None

    def upload_manifest(self, manifest: Manifest) -> bool:
        try:
            token = self.auth.upload_token(self.bucket, MANIFEST_KEY, 3600)
            tmp = json.dumps(manifest.to_dict(), ensure_ascii=False).encode("utf-8")
            tmp_path = "manifest.tmp.json"
            with open(tmp_path, "wb") as f:
                f.write(tmp)
            ret, info = put_file(token, MANIFEST_KEY, tmp_path)
            os.remove(tmp_path)
            return info.status_code == 200
        except Exception:
            return False

    # Lock
    def download_lock(self) -> Optional[Dict]:
        try:
            base_url = f"{self.domain}/{LOCK_KEY}" if self.domain else None
            if base_url:
                import requests
                private_url = self.auth.private_download_url(base_url, expires=60)
                r = requests.get(private_url, timeout=10)
                if r.status_code == 200:
                    return r.json()
                return None
            ret, info = self.bm.stat(self.bucket, LOCK_KEY)
            if info.status_code == 612:
                return None
            return None
        except Exception:
            return None

    def upload_lock(self, data: Dict) -> bool:
        try:
            token = self.auth.upload_token(self.bucket, LOCK_KEY, 3600)
            tmp_path = "lock.tmp.json"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            ret, info = put_file(token, LOCK_KEY, tmp_path)
            os.remove(tmp_path)
            return info.status_code == 200
        except Exception:
            return False

    def delete_lock(self) -> bool:
        try:
            ret, info = self.bm.delete(self.bucket, LOCK_KEY)
            return info.status_code in (200, 612)
        except Exception:
            return False

    # File operations (placeholders for MVP)
    def upload_file(self, key: str, local_path: str) -> bool:
        try:
            token = self.auth.upload_token(self.bucket, key, 3600)
            ret, info = put_file(token, key, local_path)
            return info.status_code == 200
        except Exception:
            return False

    def delete_file(self, key: str) -> bool:
        try:
            ret, info = self.bm.delete(self.bucket, key)
            return info.status_code in (200, 612)
        except Exception:
            return False 