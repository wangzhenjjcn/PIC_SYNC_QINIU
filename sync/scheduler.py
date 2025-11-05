import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict

try:
    from .config import AppConfig, app_data_dir
    from .device_id import ensure_device_id
    from .manifest import Manifest, ManifestEntry
    from .lock import LeaseLock
    from .qiniu_client import QiniuClient
    from .scanner import scan_directory
    from .sqlite_store import SQLiteStore
    from .diff import compute_diff
    from .cache import load_manifest_cache, save_manifest_cache
    from .profile import compute_profile_key, load_last_profile_key, save_last_profile_key
except ImportError:
    import sys as _sys, os as _os
    _sys.path.append(_os.path.dirname(_os.path.dirname(__file__)))
    from sync.config import AppConfig, app_data_dir
    from sync.device_id import ensure_device_id
    from sync.manifest import Manifest, ManifestEntry
    from sync.lock import LeaseLock
    from sync.qiniu_client import QiniuClient
    from sync.scanner import scan_directory
    from sync.sqlite_store import SQLiteStore
    from sync.diff import compute_diff
    from sync.cache import load_manifest_cache, save_manifest_cache
    from sync.profile import compute_profile_key, load_last_profile_key, save_last_profile_key


LOCK_GRACE_MINUTES = 5


class SyncEngine:
    def __init__(self, logger: Callable[[str], None], state_cb: Callable[[str], None]):
        self.logger = logger
        self.state_cb = state_cb
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._kick = threading.Event()
        self._cycle_count = 0
        self._profile_key: str | None = None
        self._skip_delete_once = False

    def start(self, cfg: AppConfig):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, args=(cfg,), daemon=True)
        self._thread.start()

    def stop(self):
        if self._thread and self._thread.is_alive():
            self._stop.set()
            self._kick.set()
            self._thread.join(timeout=5)

    def kick(self):
        """Trigger an immediate sync cycle when waiting for next interval."""
        try:
            self._kick.set()
            self.logger("收到立刻刷新请求")
        except Exception:
            pass

    def _run(self, cfg: AppConfig):
        ensure_device_id(cfg)
        db_path = str(app_data_dir() / "state.db")
        store = SQLiteStore(db_path)
        qn = QiniuClient(cfg.qiniu_access_key or "", cfg.qiniu_secret_key or "", cfg.qiniu_bucket or "", cfg.qiniu_domain or None, cfg.qiniu_region or None)

        # Compute profile key and detect switch
        current_key = compute_profile_key(cfg.qiniu_bucket, cfg.qiniu_domain, cfg.local_dir, getattr(cfg, "qiniu_subdir", None))
        last_key = load_last_profile_key()
        self._profile_key = current_key
        if last_key != current_key:
            self._skip_delete_once = True
            save_last_profile_key(current_key)
            self.logger("检测到配置切换(空间/域名/本地目录)，本轮将跳过远端删除")

        interval = max(1, int(cfg.scan_interval_minutes or 5)) * 60
        while not self._stop.is_set():
            try:
                self._cycle_count += 1
                self.logger(f"开始同步 第{self._cycle_count}轮")
                self.state_cb("扫描与同步中")
                self._cycle(cfg, store, qn)
                self.state_cb("空闲")
                try:
                    interval = max(1, int(cfg.scan_interval_minutes or 5)) * 60
                    self.state_cb(f"NEXT|{interval}")
                except Exception:
                    pass
            except Exception as e:
                self.logger(f"错误: {e}")
                self.state_cb("错误")
            for _ in range(interval // 1):
                if self._stop.is_set():
                    break
                if self._kick.is_set():
                    # consume and break to start next cycle immediately
                    self._kick.clear()
                    break
                time.sleep(1)

    def _cycle(self, cfg: AppConfig, store: SQLiteStore, qn: QiniuClient):
        # 1) Load manifest: prefer the newer between remote and local cache (to avoid CDN延迟导致的旧清单)
        manifest, etag = qn.download_manifest()
        cached_dict, cached_etag = load_manifest_cache(self._profile_key or "default")
        if manifest and cached_dict:
            try:
                cached_m = Manifest.from_dict(cached_dict)
                remote_ts = manifest.generated_at_utc or ""
                cached_ts = cached_m.generated_at_utc or ""
                # 比较时间字符串，ISO8601可直接比字符串，或显式解析
                if cached_ts >= remote_ts:
                    manifest = cached_m
                    etag = cached_etag
                    self.logger("检测到本地清单更新不早于远端，优先使用本地清单缓存")
                else:
                    self.logger("使用远端清单")
            except Exception:
                self.logger("清单比对失败，回退使用远端/本地可用者")
        if not manifest:
            if cached_dict:
                manifest = Manifest.from_dict(cached_dict)
                etag = cached_etag
                self.logger("使用本地清单缓存")
            else:
                manifest = Manifest.empty(cfg.device_id or "")

        # Build server index dict
        server_index = {k: vars(v) for k, v in manifest.files.items()}

        # 2) Local scan
        local_index: Dict[str, Dict] = {}
        now_iso = datetime.now(timezone.utc).isoformat()
        for r in scan_directory(cfg.local_dir or "."):
            r["modified_by_device_id"] = cfg.device_id or ""
            r["deleted"] = 0
            local_index[r["rel_path"]] = r
            store.upsert_local_file({**r, "last_scanned_at_utc": now_iso})

        # 3) Diff (based on md5)
        diff = compute_diff(local_index, server_index)
        if self._skip_delete_once:
            # clear remote deletes once after profile switch
            diff.to_delete_remote = []
        self.logger(f"待上传: {len(diff.to_upload)}, 待下载: {len(diff.to_download)}, 待删除远端: {len(diff.to_delete_remote)}")
        try:
            self.state_cb(f"COUNTS|u={len(diff.to_upload)}|d={len(diff.to_download)}|del={len(diff.to_delete_remote)}")
        except Exception:
            pass

        # Short-circuit: no changes
        if not diff.to_upload and not diff.to_delete_remote and not diff.to_download:
            self.logger("无差异，跳过锁与上传")
            try:
                self.state_cb("NO_DIFF")
            except Exception:
                pass
            self._skip_delete_once = False
            return

        # 4) Downloads (skipped in MVP)

        # 5) Lock handling or force-upload
        have_lock = False
        should_release_lock = False
        if getattr(cfg, "force_upload_ignore_lock", False):
            self.logger("已启用强制上传，忽略锁")
            have_lock = True
        else:
            remote_lock_dict = qn.download_lock() or {}
            if remote_lock_dict:
                try:
                    remote_lock = LeaseLock.from_dict(remote_lock_dict)
                    # self-lock: if same device, treat as non-conflict and renew
                    if remote_lock.owner_device_id == (cfg.device_id or ""):
                        renewal = LeaseLock.new(cfg.device_id or "", manifest.manifest_seq, ttl_minutes=15, nonce=remote_lock.nonce or uuid.uuid4().hex[:8])
                        qn.upload_lock(renewal.to_dict())
                        have_lock = True
                        should_release_lock = True
                        self.logger("检测到本机已持有锁，续租成功")
                    else:
                        # foreign lock
                        if remote_lock.is_expired_with_grace(LOCK_GRACE_MINUTES):
                            self.logger("检测到他人过期锁(含5分钟宽限)，执行清理")
                            qn.delete_lock()
                        else:
                            self.logger("锁在他人持有且未超时，跳过本轮上传")
                            return
                except Exception:
                    pass
            if not have_lock:
                nonce = uuid.uuid4().hex[:8]
                lease = LeaseLock.new(cfg.device_id or "", manifest.manifest_seq, ttl_minutes=15, nonce=nonce)
                ok = qn.upload_lock(lease.to_dict())
                if not ok:
                    self.logger("获取锁失败，跳过本轮上传")
                    return
                # trust upload success
                have_lock = True
                should_release_lock = True
                self.logger("获取锁成功")

        # 6) Uploads/Deletes
        # Prepare remote key prefix + url helpers
        prefix = (getattr(cfg, "qiniu_subdir", None) or "").strip().strip("/\\")
        def apply_prefix(k: str) -> str:
            return f"{prefix}/{k}" if prefix else k
        _domain_raw = (cfg.qiniu_domain or "").strip()
        if _domain_raw:
            _domain_base = _domain_raw.rstrip("/")
            lower = _domain_base.lower()
            if not (lower.startswith("http://") or lower.startswith("https://")):
                _domain_base = f"https://{_domain_base}"
        else:
            _domain_base = ""
        def build_url(rel_path: str) -> str:
            if not _domain_base:
                return ""
            rel_url = apply_prefix(rel_path).replace("\\", "/")
            return f"{_domain_base}/{rel_url}"

        for rel in diff.to_upload:
            try:
                self.state_cb(f"CURRENT|{rel}")
            except Exception:
                pass
            local_path = str(Path(cfg.local_dir or ".") / rel)
            key = apply_prefix(rel)
            if os.path.exists(local_path):
                if qn.upload_file(key, local_path):
                    url = build_url(rel)
                    if url:
                        self.logger(f"上传: {rel} -> {url}")
                    else:
                        self.logger(f"上传: {rel}")
                else:
                    self.logger(f"上传失败: {rel}")

        for rel in diff.to_delete_remote:
            try:
                self.state_cb(f"CURRENT|{rel}")
            except Exception:
                pass
            if qn.delete_file(apply_prefix(rel)):
                url = build_url(rel)
                if url:
                    self.logger(f"远端删除: {rel} -> {url}")
                else:
                    self.logger(f"远端删除: {rel}")
            else:
                self.logger(f"远端删除失败: {rel}")

        try:
            self.state_cb("CURRENT|")  # clear current file
        except Exception:
            pass

        # 7) Rewrite manifest and upload
        new_manifest = Manifest(
            version=1,
            manifest_seq=manifest.manifest_seq + 1,
            generated_at_utc=datetime.now(timezone.utc).isoformat(),
            generator_device_id=cfg.device_id or "",
            files={}
        )
        for rel, r in local_index.items():
            new_manifest.files[rel] = ManifestEntry(
                rel_path=rel,
                size=r.get("size", 0),
                mtime_utc=r.get("mtime_utc", ""),
                md5=r.get("md5", ""),
                qetag=r.get("qetag", ""),
                ext=r.get("ext", ""),
                modified_by_device_id=cfg.device_id or "",
                deleted=r.get("deleted", 0),
            )
        if qn.upload_manifest(new_manifest):
            self.logger("清单已更新")
            # Save cache after successful upload to reduce future downloads
            try:
                save_manifest_cache(self._profile_key or "default", new_manifest.to_dict(), etag)
            except Exception:
                pass
        else:
            self.logger("清单上传失败")

        # 8) Release lock if acquired
        if should_release_lock:
            qn.delete_lock()
            self.logger("锁已释放")
        else:
            self.logger("未创建锁，跳过释放")

        # clear once-only delete skip after a successful cycle
        self._skip_delete_once = False 