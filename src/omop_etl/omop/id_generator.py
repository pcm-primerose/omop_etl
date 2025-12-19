import hashlib


def sha1_bigint(namespace: str, value: str) -> int:
    h = hashlib.sha1(f"{namespace}:{value}".encode("utf-8")).digest()
    n = int.from_bytes(h[:8], "big", signed=False)
    return n & ((1 << 63) - 1)
