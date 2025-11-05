from typing import Dict, List, Tuple


class DiffResult:
    def __init__(self):
        self.to_upload: List[str] = []
        self.to_download: List[str] = []
        self.to_delete_remote: List[str] = []
        self.conflicts: List[str] = []


def compute_diff(local: Dict[str, Dict], server: Dict[str, Dict]) -> DiffResult:
    r = DiffResult()
    local_keys = set(local.keys())
    server_keys = set(server.keys())

    # New or modified locally (by md5)
    for k in sorted(local_keys - server_keys):
        r.to_upload.append(k)
    for k in sorted(local_keys & server_keys):
        if (local[k] or {}).get("md5") != (server[k] or {}).get("md5"):
            r.to_upload.append(k)

    # Present on server but missing locally -> delete on remote
    for k in sorted(server_keys - local_keys):
        if not (server[k] or {}).get("deleted"):
            r.to_delete_remote.append(k)

    return r 