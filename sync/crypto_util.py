import base64
import hashlib
import hmac
import os
from typing import Tuple


# 固定密码（按需求约定）
PASSWORD = "Myazure"


def _derive_key(password: str) -> bytes:
    return hashlib.sha256(password.encode("utf-8")).digest()


def _keystream(key: bytes, iv: bytes, length: int) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < length:
        ctr = counter.to_bytes(8, "big")
        block = hmac.new(key, iv + ctr, hashlib.sha256).digest()
        out.extend(block)
        counter += 1
    return bytes(out[:length])


def _seal(payload: bytes, key: bytes) -> bytes:
    tag = hmac.new(key, payload, hashlib.sha256).digest()
    return payload + tag


def _open(sealed: bytes, key: bytes) -> Tuple[bool, bytes]:
    if len(sealed) < 32:
        return False, b""
    payload, tag = sealed[:-32], sealed[-32:]
    expect = hmac.new(key, payload, hashlib.sha256).digest()
    if not hmac.compare_digest(tag, expect):
        return False, b""
    return True, payload


def encrypt_to_base64(plaintext: bytes, password: str = PASSWORD) -> str:
    key = _derive_key(password)
    iv = os.urandom(16)
    ks = _keystream(key, iv, len(plaintext))
    ct = bytes([a ^ b for a, b in zip(plaintext, ks)])
    header = b"CFG1"  # 版本头
    sealed = _seal(header + iv + ct, key)
    return base64.urlsafe_b64encode(sealed).decode("ascii")


def decrypt_from_base64(token: str, password: str = PASSWORD) -> bytes:
    key = _derive_key(password)
    raw = base64.urlsafe_b64decode(token.encode("ascii"))
    ok, payload = _open(raw, key)
    if not ok:
        raise ValueError("密文校验失败，可能口令不正确或数据损坏")
    if not payload.startswith(b"CFG1"):
        raise ValueError("密文版本不支持")
    iv = payload[4:20]
    ct = payload[20:]
    ks = _keystream(key, iv, len(ct))
    pt = bytes([a ^ b for a, b in zip(ct, ks)])
    return pt


