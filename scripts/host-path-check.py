#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Fail if a built artifact embeds a build-host path.

Usage: host-path-check.py ARTIFACT PATH...

ARTIFACT is a zip archive (every member is read) or any other file. Each member
is searched as raw bytes and also with its JSON string escapes (\\uXXXX, \\\\,
\\/) and URL percent-encoding decoded, alone and together, so a path written
as Jane%20Doe, J\\u00f6rg or J%C3%B6rg is still found. Each PATH is a
build-host directory (the checkout, $HOME, etc.). A match is that directory
followed by a separator.
"""

import re
import sys
import zipfile

NOT_PRECEDED = rb"(?:(?<=file://)|(?<![A-Za-z0-9._~/\\-]))"
SEPARATOR = rb"(?:/|\\\\|\\|%2[Ff]|%5[Cc])+"


def needles(paths):
    out = []
    for path in paths:
        parts = [c for c in re.split(r"[/\\]", path) if c]
        if not parts:
            continue
        drive = re.fullmatch(r"([A-Za-z]):", parts[0])
        if drive:
            head = (
                rb"(?:/)?["
                + drive.group(1).upper().encode()
                + drive.group(1).lower().encode()
                + rb"]:"
            )
            names = parts[1:]
        else:
            head = b""
            names = parts
        if not names:
            continue
        body = SEPARATOR.join(re.escape(n.encode()) for n in names)
        lead = head + SEPARATOR if head else SEPARATOR
        prefix = NOT_PRECEDED if len(names) < 2 and not head else b""
        out.append(re.compile(prefix + lead + body + SEPARATOR))
    return out


JSON_ESCAPE = re.compile(rb"\\(u[0-9A-Fa-f]{4}|[\\/\"bfnrt])")
JSON_SURROGATE_PAIR = re.compile(
    rb"\\u(d[89ab][0-9a-f]{2})\\u(d[c-f][0-9a-f]{2})", re.I
)
PERCENT = re.compile(rb"%([0-9A-Fa-f]{2})")
SIMPLE = {
    b"\\": b"\\",
    b"/": b"/",
    b'"': b'"',
    b"b": b"\b",
    b"f": b"\f",
    b"n": b"\n",
    b"r": b"\r",
    b"t": b"\t",
}


def json_unescape(data):
    def pair(m):
        hi, lo = int(m.group(1), 16), int(m.group(2), 16)
        return chr(0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)).encode(
            "utf-8", "surrogatepass"
        )

    def one(m):
        e = m.group(1)
        if e[:1] == b"u":
            return chr(int(e[1:], 16)).encode("utf-8", "surrogatepass")
        return SIMPLE[e]

    return JSON_ESCAPE.sub(one, JSON_SURROGATE_PAIR.sub(pair, data))


def percent_decode(data):
    return PERCENT.sub(lambda m: bytes([int(m.group(1), 16)]), data)


def views(data):
    seen = []
    for view in (
        data,
        json_unescape(data),
        percent_decode(data),
        percent_decode(json_unescape(data)),
        json_unescape(percent_decode(data)),
    ):
        if view not in seen:
            seen.append(view)
    return seen


def members(artifact):
    if zipfile.is_zipfile(artifact):
        with zipfile.ZipFile(artifact) as z:
            for name in z.namelist():
                yield name, z.read(name)
    else:
        with open(artifact, "rb") as f:
            yield artifact.replace("\\", "/").rsplit("/", 1)[-1], f.read()


def main(argv):
    if len(argv) < 3:
        sys.exit("usage: host-path-check.py ARTIFACT PATH...")
    patterns = needles(argv[2:])
    for name, data in members(argv[1]):
        for view in views(data):
            for pattern in patterns:
                m = pattern.search(view)
                if m:
                    found = view[m.start() : m.end() + 40].decode(
                        "ascii", "backslashreplace"
                    )
                    sys.exit(f"{name} embeds a build-host path: {found}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
