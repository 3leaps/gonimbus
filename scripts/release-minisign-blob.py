#!/usr/bin/env python3
"""Print the public-blob SHA-256 of a minisign public key file.

The value is the lowercase hex SHA-256 of the base64-decoded public key blob
(2-byte algorithm "Ed" or "ED", 8-byte key ID, 32-byte Ed25519 key; 42 bytes).
The optional single "untrusted comment:" line and surrounding whitespace are
not hashed. Anything that is not exactly one such public key is refused, so a
secret key file never yields a fingerprint.
"""

import base64
import binascii
import hashlib
import sys
from pathlib import Path

MAX_BYTES = 4 * 1024
BLOB_LEN = 2 + 8 + 32
COMMENT = "untrusted comment:"


def public_blob(data: bytes) -> bytes:
    if not data or len(data) >= MAX_BYTES:
        raise ValueError("not a minisign public key")
    try:
        text = data.decode("utf-8")
    except UnicodeError as error:
        raise ValueError("not a minisign public key") from error
    if "secret key" in text.lower():
        raise ValueError("private material in minisign public export")
    blob = None
    comment = False
    for raw in text.split("\n"):
        line = raw.strip()
        if not line:
            continue
        if line.lower().startswith(COMMENT):
            if blob is not None or comment:
                raise ValueError("not a minisign public key")
            comment = True
            continue
        try:
            decoded = base64.b64decode(line, validate=True)
        except (binascii.Error, ValueError) as error:
            raise ValueError("not a minisign public key") from error
        if len(decoded) != BLOB_LEN or decoded[:2] not in (b"Ed", b"ED"):
            raise ValueError("not a minisign public key")
        if blob is not None:
            raise ValueError("not a minisign public key")
        blob = decoded
    if blob is None:
        raise ValueError("not a minisign public key")
    return blob


def main() -> None:
    if len(sys.argv) != 2:
        raise ValueError("expected one minisign public key path")
    path = Path(sys.argv[1])
    if path.is_symlink() or not path.is_file():
        raise ValueError("minisign public export must be a regular file")
    print(hashlib.sha256(public_blob(path.read_bytes())).hexdigest())


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        detail = (
            str(error)
            if isinstance(error, ValueError)
            else "unreadable minisign public export"
        )
        print(f"error: {detail}", file=sys.stderr)
        sys.exit(1)
