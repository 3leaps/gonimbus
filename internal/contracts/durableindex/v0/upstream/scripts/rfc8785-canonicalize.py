#!/usr/bin/env python3
"""RFC 8785 JSON Canonicalization Scheme (JCS).

Standard-library only. Reads a JSON document (or strict LF-terminated NDJSON
stream with --ndjson-array) from stdin or a file path and writes canonical
UTF-8 bytes (or --digest / --vector / --check verdict) to stdout.

Numbers follow ECMA-262 Number::toString (RFC 8785 §3.2.2.3) and are
restricted to the I-JSON numeric domain (RFC 7493): finite IEEE 754
binary64 values, with integer tokens admitted only when their exact mathematical
value is an IEEE 754 binary64 value or when the token is already the canonical
ECMAScript spelling of its nearest binary64. Non-finite values and imprecise
integers are rejected. Object member names must be unique at every nesting
depth. Strings must be well-formed Unicode scalar values and must not contain
lone surrogates or any of the 66 Unicode noncharacters forbidden by RFC 7493
§2.1 (U+FDD0..U+FDEF and U+..FFFE / U+..FFFF across planes 0..16). Non-control
Unicode, including U+2028 and U+2029, is serialized as unescaped UTF-8.

This is the stdlib conformance-gate witness for Crucible contracts, aligned
with the canonprims jcs/rfc8785 domain and CLI surface. It is not a consumer
library. jq -S is not JCS and must not be used as the digest oracle.
"""

from __future__ import annotations

import hashlib
import math
import sys
from typing import Any, BinaryIO

DEFAULT_MAX_BYTES = 16_777_216
DEFAULT_MAX_DEPTH = 128
DEFAULT_MAX_OBJECT_MEMBERS = 1_048_576
DEFAULT_MAX_STRING_BYTES = 16_777_216

EXIT_OK = 0
EXIT_REFUSED = 1
EXIT_USAGE = 2
EXIT_INPUT = 4
EXIT_OUTPUT = 5

_WS_CHARS = " \t\n\r"
_SIMPLE_ESCAPES = {
    '"': '"',
    "\\": "\\",
    "/": "/",
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
}
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")

_ENC_VAL = 0
_ENC_LIT = 1
_ENC_KEY = 2


def is_unicode_noncharacter(code: int) -> bool:
    """Return True if code is one of the 66 Unicode noncharacters (RFC 7493 §2.1)."""
    return (0xFDD0 <= code <= 0xFDEF) or (
        0 <= code <= 0x10FFFF and (code & 0xFFFF) in (0xFFFE, 0xFFFF)
    )


def _validate_ijson_string(value: str, max_string_bytes: int | None = None) -> None:
    for char in value:
        code = ord(char)
        if 0xD800 <= code <= 0xDFFF:
            raise ValueError("lone_surrogate: lone surrogate is not permitted in JCS")
        if is_unicode_noncharacter(code):
            raise ValueError(
                f"noncharacter: Unicode noncharacter U+{code:04X} is not permitted in I-JSON / JCS"
            )
    if max_string_bytes is not None and len(value.encode("utf-8")) > max_string_bytes:
        raise ValueError("too_large: decoded string exceeds max_string_bytes")


def _reject_constant(token: str) -> Any:
    raise ValueError(f"invalid_json: non-JSON constant is not permitted: {token}")


def _parse_ijson_float(token: str) -> float:
    value = float(token)
    if not math.isfinite(value):
        raise ValueError(
            "non_finite_number: non-finite numbers are not permitted in JCS"
        )
    return value


def _parse_ijson_int(token: str) -> int:
    digits = token[1:] if token.startswith("-") else token
    # The largest finite IEEE 754 binary64 has 309 integer digits. Rejecting
    # longer integer tokens before int() avoids Python's 4300-digit conversion
    # ceiling and enforces the I-JSON binary64 integer domain deterministically.
    if len(digits) > 309:
        raise ValueError(
            "integer_outside_binary64: integer is outside the I-JSON IEEE 754 binary64 domain"
        )
    nearest = float(token)
    if not math.isfinite(nearest):
        raise ValueError(
            "integer_outside_binary64: integer is outside the I-JSON IEEE 754 binary64 domain"
        )
    exact_val = int(token)
    nearest_int = int(nearest)
    if nearest_int == exact_val:
        return exact_val
    # Canonical output must remain admissible as input (e.g. 2^64 canonicalizes
    # to 18446744073709552000 under ES6 Number::toString).
    if _encode_number(nearest) == token:
        return nearest_int
    raise ValueError(
        "integer_outside_binary64: integer is outside the I-JSON IEEE 754 binary64 domain"
    )


def _unique_object(pairs: list[tuple[Any, Any]]) -> dict[Any, Any]:
    # I-JSON / RFC 8785: object member names are unique at every nesting depth.
    # json.loads otherwise keeps the last duplicate and silently changes the
    # document that would be signed or digested.
    result: dict[Any, Any] = {}
    for key, value in pairs:
        _validate_ijson_string(key)
        if key in result:
            raise ValueError("duplicate_member: duplicate object member name")
        result[key] = value
    return result


def _skip_ws(raw: str, pos: int, n: int) -> int:
    while pos < n and raw[pos] in _WS_CHARS:
        pos += 1
    return pos


def _parse_hex4(raw: str, pos: int, n: int) -> int:
    if pos + 4 > n:
        raise ValueError("invalid_json: incomplete \\uXXXX escape")
    chunk = raw[pos : pos + 4]
    if any(ch not in _HEX_DIGITS for ch in chunk):
        raise ValueError("invalid_json: invalid \\uXXXX escape")
    return int(chunk, 16)


def _utf8_scalar_len(code: int) -> int:
    if code <= 0x7F:
        return 1
    if code <= 0x7FF:
        return 2
    if code <= 0xFFFF:
        return 3
    return 4


def _parse_string_token(
    raw: str, pos: int, n: int, max_string_bytes: int | None
) -> tuple[str, int]:
    pos += 1  # consume opening '"'
    chars: list[str] = []
    utf8_bytes = 0
    while pos < n:
        ch = raw[pos]
        if ch == '"':
            return "".join(chars), pos + 1
        code = ord(ch)
        if code < 0x20:
            raise ValueError("invalid_json: unescaped control character in string")
        if ch != "\\":
            if 0xD800 <= code <= 0xDFFF:
                raise ValueError(
                    "lone_surrogate: lone surrogate is not permitted in JCS"
                )
            if is_unicode_noncharacter(code):
                raise ValueError(
                    f"noncharacter: Unicode noncharacter U+{code:04X} is not permitted in I-JSON / JCS"
                )
            utf8_bytes += _utf8_scalar_len(code)
            if max_string_bytes is not None and utf8_bytes > max_string_bytes:
                raise ValueError("too_large: decoded string exceeds max_string_bytes")
            chars.append(ch)
            pos += 1
            continue

        pos += 1
        if pos >= n:
            raise ValueError("invalid_json: unterminated escape sequence")
        esc = raw[pos]
        pos += 1
        if esc in _SIMPLE_ESCAPES:
            decoded = _SIMPLE_ESCAPES[esc]
            scalar = ord(decoded)
        elif esc == "u":
            u1 = _parse_hex4(raw, pos, n)
            pos += 4
            if 0xDC00 <= u1 <= 0xDFFF:
                raise ValueError(
                    "lone_surrogate: lone low surrogate is not permitted in JCS"
                )
            if 0xD800 <= u1 <= 0xDBFF:
                if pos + 2 > n or raw[pos : pos + 2] != "\\u":
                    raise ValueError(
                        "lone_surrogate: lone high surrogate is not permitted in JCS"
                    )
                pos += 2
                u2 = _parse_hex4(raw, pos, n)
                pos += 4
                if not (0xDC00 <= u2 <= 0xDFFF):
                    raise ValueError(
                        "lone_surrogate: lone high surrogate is not permitted in JCS"
                    )
                scalar = 0x10000 + ((u1 - 0xD800) << 10) + (u2 - 0xDC00)
            else:
                scalar = u1
            decoded = chr(scalar)
        else:
            raise ValueError("invalid_json: invalid escape sequence")

        if is_unicode_noncharacter(scalar):
            raise ValueError(
                f"noncharacter: Unicode noncharacter U+{scalar:04X} is not permitted in I-JSON / JCS"
            )
        utf8_bytes += _utf8_scalar_len(scalar)
        if max_string_bytes is not None and utf8_bytes > max_string_bytes:
            raise ValueError("too_large: decoded string exceeds max_string_bytes")
        chars.append(decoded)

    raise ValueError("invalid_json: unterminated string")


def _parse_number_token(raw: str, pos: int, n: int) -> tuple[int | float, int]:
    start = pos
    if raw[pos] == "-":
        pos += 1
    if pos >= n or not ("0" <= raw[pos] <= "9"):
        raise ValueError("invalid_json: invalid number")
    if raw[pos] == "0":
        pos += 1
        if pos < n and "0" <= raw[pos] <= "9":
            raise ValueError("invalid_json: leading zero in number")
    else:
        while pos < n and "0" <= raw[pos] <= "9":
            pos += 1

    is_int = True
    if pos < n and raw[pos] == ".":
        is_int = False
        pos += 1
        frac_start = pos
        while pos < n and "0" <= raw[pos] <= "9":
            pos += 1
        if pos == frac_start:
            raise ValueError("invalid_json: missing fraction digits in number")

    if pos < n and raw[pos] in ("e", "E"):
        is_int = False
        pos += 1
        if pos < n and raw[pos] in ("+", "-"):
            pos += 1
        exp_start = pos
        while pos < n and "0" <= raw[pos] <= "9":
            pos += 1
        if pos == exp_start:
            raise ValueError("invalid_json: missing exponent digits in number")

    token = raw[start:pos]
    if is_int:
        return _parse_ijson_int(token), pos
    return _parse_ijson_float(token), pos


def _parse_object_member_head(
    raw: str,
    pos: int,
    n: int,
    obj: dict[str, Any],
    *,
    max_object_members: int | None,
    max_string_bytes: int | None,
) -> tuple[str, int]:
    if pos >= n or raw[pos] != '"':
        raise ValueError("invalid_json: expected quoted object member name")
    if max_object_members is not None and len(obj) >= max_object_members:
        raise ValueError("too_large: object member count exceeds max_object_members")
    key, pos = _parse_string_token(raw, pos, n, max_string_bytes)
    if key in obj:
        raise ValueError("duplicate_member: duplicate object member name")
    pos = _skip_ws(raw, pos, n)
    if pos >= n or raw[pos] != ":":
        raise ValueError("invalid_json: expected ':' after object member name")
    return key, pos + 1


def jcs_loads(
    raw: str,
    *,
    max_depth: int | None = DEFAULT_MAX_DEPTH,
    max_object_members: int | None = DEFAULT_MAX_OBJECT_MEMBERS,
    max_string_bytes: int | None = DEFAULT_MAX_STRING_BYTES,
) -> Any:
    """Iterative RFC 8259 + I-JSON loader with deterministic left-to-right defect detection."""
    if not isinstance(raw, str):
        raise TypeError("jcs_loads requires a str")
    n = len(raw)
    if n > 0 and raw[0] == "\ufeff":
        raise ValueError("invalid_json: UTF-8 BOM is not permitted")

    pos = _skip_ws(raw, 0, n)
    frames: list[list[Any]] = []
    need_value = True
    val: Any = None

    while True:
        if need_value:
            pos = _skip_ws(raw, pos, n)
            if pos >= n:
                raise ValueError("invalid_json: unexpected end of input")
            ch = raw[pos]
            if ch == "[":
                if max_depth is not None and len(frames) >= max_depth:
                    raise ValueError("too_deep: container nesting exceeds max_depth")
                pos = _skip_ws(raw, pos + 1, n)
                if pos < n and raw[pos] == "]":
                    val = []
                    pos += 1
                    need_value = False
                    continue
                frames.append(["arr", []])
                continue
            if ch == "{":
                if max_depth is not None and len(frames) >= max_depth:
                    raise ValueError("too_deep: container nesting exceeds max_depth")
                pos = _skip_ws(raw, pos + 1, n)
                if pos < n and raw[pos] == "}":
                    val = {}
                    pos += 1
                    need_value = False
                    continue
                obj: dict[str, Any] = {}
                key, pos = _parse_object_member_head(
                    raw,
                    pos,
                    n,
                    obj,
                    max_object_members=max_object_members,
                    max_string_bytes=max_string_bytes,
                )
                frames.append(["obj", obj, key])
                continue
            if ch == '"':
                val, pos = _parse_string_token(raw, pos, n, max_string_bytes)
            elif ch == "t":
                if raw[pos : pos + 4] != "true":
                    raise ValueError("invalid_json: invalid literal")
                val = True
                pos += 4
            elif ch == "f":
                if raw[pos : pos + 5] != "false":
                    raise ValueError("invalid_json: invalid literal")
                val = False
                pos += 5
            elif ch == "n":
                if raw[pos : pos + 4] != "null":
                    raise ValueError("invalid_json: invalid literal")
                val = None
                pos += 4
            elif ch == "-" or ("0" <= ch <= "9"):
                val, pos = _parse_number_token(raw, pos, n)
            else:
                raise ValueError("invalid_json: unexpected character")
            need_value = False
            continue

        if not frames:
            pos = _skip_ws(raw, pos, n)
            if pos != n:
                raise ValueError("invalid_json: trailing data after top-level value")
            return val

        top = frames[-1]
        if top[0] == "arr":
            top[1].append(val)
        else:
            top[1][top[2]] = val

        pos = _skip_ws(raw, pos, n)
        if pos >= n:
            raise ValueError("invalid_json: unterminated container")
        delim = raw[pos]
        if delim == ",":
            pos = _skip_ws(raw, pos + 1, n)
            if top[0] == "obj":
                key, pos = _parse_object_member_head(
                    raw,
                    pos,
                    n,
                    top[1],
                    max_object_members=max_object_members,
                    max_string_bytes=max_string_bytes,
                )
                top[2] = key
            need_value = True
        elif delim == "]" and top[0] == "arr":
            pos += 1
            val = top[1]
            frames.pop()
        elif delim == "}" and top[0] == "obj":
            pos += 1
            val = top[1]
            frames.pop()
        else:
            raise ValueError("invalid_json: expected ',' or closing bracket")


def jcs_dumps(value: Any) -> str:
    return _encode(value)


def jcs_sort_set_bytes(elements: Any) -> bytes:
    """Canonicalize a top-level list as a duplicate-free JCS set sorted by element UTF-8 bytes."""
    if not isinstance(elements, list):
        raise ValueError("sort_set: top-level value must be a JSON array")
    encoded = [jcs_dumps(item).encode("utf-8") for item in elements]
    encoded.sort()
    for idx in range(1, len(encoded)):
        if encoded[idx] == encoded[idx - 1]:
            raise ValueError(
                "duplicate_set_element: duplicate element in canonical set"
            )
    return b"[" + b",".join(encoded) + b"]"


def _require_str_key(key: Any) -> str:
    if not isinstance(key, str):
        raise TypeError(f"JCS object member name must be str, got {type(key)!r}")
    return key


def _encode(root: Any) -> str:
    """Iterative RFC 8785 serializer (no Python recursion depth limit)."""
    parts: list[str] = []
    stack: list[tuple[int, Any]] = [(_ENC_VAL, root)]
    while stack:
        op, node = stack.pop()
        if op == _ENC_LIT:
            parts.append(node)
            continue
        if op == _ENC_KEY:
            parts.append(_encode_string(node))
            parts.append(":")
            continue
        if node is None:
            parts.append("null")
        elif node is True:
            parts.append("true")
        elif node is False:
            parts.append("false")
        elif isinstance(node, str):
            parts.append(_encode_string(node))
        elif isinstance(node, int) and not isinstance(node, bool):
            parts.append(_encode_number(node))
        elif isinstance(node, float):
            parts.append(_encode_number(node))
        elif isinstance(node, list):
            if not node:
                parts.append("[]")
            else:
                parts.append("[")
                stack.append((_ENC_LIT, "]"))
                for idx in range(len(node) - 1, -1, -1):
                    stack.append((_ENC_VAL, node[idx]))
                    if idx > 0:
                        stack.append((_ENC_LIT, ","))
        elif isinstance(node, dict):
            if not node:
                parts.append("{}")
            else:
                keys = sorted(
                    node.keys(),
                    key=lambda key: _require_str_key(key).encode("utf-16-be"),
                )
                parts.append("{")
                stack.append((_ENC_LIT, "}"))
                for idx in range(len(keys) - 1, -1, -1):
                    k = keys[idx]
                    stack.append((_ENC_VAL, node[k]))
                    stack.append((_ENC_KEY, k))
                    if idx > 0:
                        stack.append((_ENC_LIT, ","))
        else:
            raise TypeError(f"unsupported type for JCS: {type(node)!r}")
    return "".join(parts)


def _encode_string(value: str) -> str:
    # ECMA-262 QuoteJSONString / JSON.stringify (RFC 8785 §3.2.2.2).
    chunks = ['"']
    for char in value:
        code = ord(char)
        if char == '"':
            chunks.append('\\"')
        elif char == "\\":
            chunks.append("\\\\")
        elif char == "\b":
            chunks.append("\\b")
        elif char == "\f":
            chunks.append("\\f")
        elif char == "\n":
            chunks.append("\\n")
        elif char == "\r":
            chunks.append("\\r")
        elif char == "\t":
            chunks.append("\\t")
        elif 0xD800 <= code <= 0xDFFF:
            raise ValueError("lone_surrogate: lone surrogate is not permitted in JCS")
        elif is_unicode_noncharacter(code):
            raise ValueError(
                f"noncharacter: Unicode noncharacter U+{code:04X} is not permitted in I-JSON / JCS"
            )
        elif code < 0x20:
            chunks.append(f"\\u{code:04x}")
        else:
            # Non-control Unicode, including U+2028 / U+2029, is unescaped UTF-8.
            chunks.append(char)
    chunks.append('"')
    return "".join(chunks)


def _require_ijson_number(value: int | float) -> float:
    if isinstance(value, bool):
        raise TypeError("bool is not a JCS number")
    if isinstance(value, int):
        try:
            as_float = float(value)
        except OverflowError as exc:
            raise ValueError(
                "integer_outside_binary64: integer is outside the I-JSON IEEE 754 binary64 domain"
            ) from exc
        if not math.isfinite(as_float) or int(as_float) != value:
            raise ValueError(
                "integer_outside_binary64: integer is outside the I-JSON IEEE 754 binary64 domain"
            )
        return as_float
    if not math.isfinite(value):
        raise ValueError(
            "non_finite_number: non-finite numbers are not permitted in JCS"
        )
    return value


def _encode_number(value: int | float) -> str:
    """ECMA-262 Number::toString as required by RFC 8785 §3.2.2.3.

    Digit sequences come from Python's shortest round-trip str(float);
    exponent zero-padding and the 1e-6 / 1e21 notation thresholds are
    then applied so the result matches ES6/V8, not json.dumps(float).
    """
    number = _require_ijson_number(value)
    if number == 0.0:
        return "0"

    rendered = str(number)
    if "n" in rendered:
        raise ValueError(
            "non_finite_number: non-finite numbers are not permitted in JCS"
        )

    sign = ""
    if rendered[0] == "-":
        sign = "-"
        rendered = rendered[1:]

    exp_str = ""
    exp_val = 0
    exp_at = rendered.find("e")
    if exp_at > 0:
        exp_str = rendered[exp_at:]
        # Python writes e-07; ECMAScript writes e-7.
        if exp_str[2:3] == "0":
            exp_str = exp_str[:2] + exp_str[3:]
        rendered = rendered[:exp_at]
        exp_val = int(exp_str[1:])

    first = rendered
    dot = ""
    last = ""
    dot_at = rendered.find(".")
    if dot_at > 0:
        dot = "."
        first = rendered[:dot_at]
        last = rendered[dot_at + 1 :]
    if last == "0":
        dot = ""
        last = ""

    # k <= n <= 21: integer decimal, no exponent.
    if 0 < exp_val < 21:
        first += last
        last = ""
        dot = ""
        exp_str = ""
        pad = exp_val - len(first)
        while pad >= 0:
            pad -= 1
            first += "0"
    # -6 < n <= 0: 0.000... form (1e-6 threshold).
    elif -7 < exp_val < 0:
        last = first + last
        first = "0"
        dot = "."
        exp_str = ""
        lead = exp_val
        while lead < -1:
            lead += 1
            last = "0" + last

    return sign + first + dot + last + exp_str


def _usage_text() -> str:
    return (
        "usage: rfc8785-canonicalize.py [options] [input.json|-]\n"
        "options:\n"
        "  --check                 exit 0 only if input is already canonical JCS\n"
        "  --digest                write sha256:<hex> of canonical bytes + newline\n"
        "  --vector                write canonical preimage + byte_length + digest JSON\n"
        "  --ndjson-array          frame strict LF-terminated NDJSON lines as a JCS array\n"
        "  --head N                with --ndjson-array, read only the first N lines (N >= 1)\n"
        "  --sort-set              sort top-level array by element JCS bytes; refuse duplicates\n"
        "  --max-bytes BYTES       maximum input byte count (default: 16777216)\n"
        "  --max-depth DEPTH       maximum container nesting depth (default: 128)\n"
        "  --max-object-members N  maximum members per object (default: 1048576)\n"
        "  --max-string-bytes N    maximum decoded UTF-8 bytes per string (default: 16777216)\n"
    )


def _parse_ascii_uint(flag: str, raw_val: str, *, min_val: int = 0) -> int:
    if not raw_val or not raw_val.isascii() or not raw_val.isdigit():
        raise ValueError(f"invalid integer for {flag}: {raw_val!r}")
    parsed = int(raw_val, 10)
    if parsed < min_val:
        raise ValueError(f"{flag} must be >= {min_val}")
    return parsed


def _parse_cli_args(argv: list[str]) -> tuple[dict[str, Any] | None, int]:
    opts: dict[str, Any] = {
        "file": "-",
        "check": False,
        "digest": False,
        "vector": False,
        "ndjson_array": False,
        "head": None,
        "sort_set": False,
        "max_bytes": DEFAULT_MAX_BYTES,
        "max_depth": DEFAULT_MAX_DEPTH,
        "max_object_members": DEFAULT_MAX_OBJECT_MEMBERS,
        "max_string_bytes": DEFAULT_MAX_STRING_BYTES,
    }
    positional: list[str] = []
    idx = 1
    while idx < len(argv):
        arg = argv[idx]
        if arg in ("-h", "--help"):
            sys.stdout.write(_usage_text())
            return None, EXIT_OK
        if arg == "--":
            positional.extend(argv[idx + 1 :])
            break
        if arg == "--check":
            opts["check"] = True
        elif arg == "--digest":
            opts["digest"] = True
        elif arg == "--vector":
            opts["vector"] = True
        elif arg == "--ndjson-array":
            opts["ndjson_array"] = True
        elif arg == "--sort-set":
            opts["sort_set"] = True
        elif arg in (
            "--head",
            "--max-bytes",
            "--max-depth",
            "--max-object-members",
            "--max-string-bytes",
        ) or arg.startswith(
            (
                "--head=",
                "--max-bytes=",
                "--max-depth=",
                "--max-object-members=",
                "--max-string-bytes=",
            )
        ):
            if "=" in arg:
                flag, val_str = arg.split("=", 1)
            else:
                flag = arg
                idx += 1
                if idx >= len(argv):
                    sys.stderr.write(f"jcs: missing value for {flag}\n{_usage_text()}")
                    return None, EXIT_USAGE
                val_str = argv[idx]
            try:
                if flag == "--head":
                    opts["head"] = _parse_ascii_uint(flag, val_str, min_val=1)
                elif flag == "--max-bytes":
                    opts["max_bytes"] = _parse_ascii_uint(flag, val_str)
                elif flag == "--max-depth":
                    opts["max_depth"] = _parse_ascii_uint(flag, val_str)
                elif flag == "--max-object-members":
                    opts["max_object_members"] = _parse_ascii_uint(flag, val_str)
                elif flag == "--max-string-bytes":
                    opts["max_string_bytes"] = _parse_ascii_uint(flag, val_str)
            except ValueError as exc:
                sys.stderr.write(f"jcs: {exc}\n{_usage_text()}")
                return None, EXIT_USAGE
        elif arg.startswith("-") and arg != "-":
            sys.stderr.write(f"jcs: unknown option: {arg}\n{_usage_text()}")
            return None, EXIT_USAGE
        else:
            positional.append(arg)
        idx += 1

    if len(positional) > 1:
        sys.stderr.write(f"jcs: too many positional arguments\n{_usage_text()}")
        return None, EXIT_USAGE
    if positional:
        opts["file"] = positional[0]

    out_modes = int(opts["check"]) + int(opts["digest"]) + int(opts["vector"])
    if out_modes > 1:
        sys.stderr.write(
            "jcs: --check, --digest, and --vector are mutually exclusive\n"
        )
        return None, EXIT_USAGE
    if opts["ndjson_array"] and opts["check"]:
        sys.stderr.write("jcs: --ndjson-array cannot be combined with --check\n")
        return None, EXIT_USAGE
    if opts["head"] is not None and not opts["ndjson_array"]:
        sys.stderr.write("jcs: --head requires --ndjson-array\n")
        return None, EXIT_USAGE

    return opts, EXIT_OK


def _read_bounded_bytes(stream: BinaryIO, max_bytes: int) -> bytes:
    data = stream.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise ValueError("too_large: input exceeds max_bytes")
    return data


def _read_ndjson_prefix_lines(
    stream: BinaryIO, *, head: int | None, max_bytes: int
) -> list[bytes]:
    lines: list[bytes] = []
    total_bytes = 0
    while head is None or len(lines) < head:
        remaining = max_bytes - total_bytes
        line = stream.readline(remaining + 1)
        if not line:
            break
        total_bytes += len(line)
        if total_bytes > max_bytes:
            raise ValueError("too_large: NDJSON input exceeds max_bytes")
        if not line.endswith(b"\n"):
            raise ValueError(
                "invalid_json: unterminated NDJSON line (missing trailing LF)"
            )
        if line.endswith(b"\r\n"):
            raise ValueError(
                "invalid_json: CRLF line ending is not permitted in NDJSON"
            )
        body = line[:-1]
        if not body or not body.strip(b" \t\r"):
            raise ValueError(
                "invalid_json: empty or blank NDJSON line is not permitted"
            )
        lines.append(body)

    if head is not None and len(lines) < head:
        raise ValueError(
            f"invalid_json: expected at least {head} NDJSON lines, found {len(lines)}"
        )
    if not lines:
        raise ValueError("invalid_json: empty NDJSON input")
    return lines


def main(argv: list[str]) -> int:
    opts, status = _parse_cli_args(argv)
    if opts is None:
        return status

    max_bytes: int = opts["max_bytes"]
    max_depth: int = opts["max_depth"]
    max_object_members: int = opts["max_object_members"]
    max_string_bytes: int = opts["max_string_bytes"]
    target: str = opts["file"]

    raw_bytes: bytes = b""
    try:
        if opts["ndjson_array"]:
            if max_depth < 1:
                raise ValueError("too_deep: NDJSON array requires max_depth >= 1")
            elem_max_depth = max_depth - 1
            if target == "-":
                raw_lines = _read_ndjson_prefix_lines(
                    sys.stdin.buffer, head=opts["head"], max_bytes=max_bytes
                )
            else:
                try:
                    with open(target, "rb") as f:
                        raw_lines = _read_ndjson_prefix_lines(
                            f, head=opts["head"], max_bytes=max_bytes
                        )
                except OSError as exc:
                    sys.stderr.write(f"jcs: cannot read {target}: {exc}\n")
                    return EXIT_INPUT
            elements: list[Any] = []
            for raw_line in raw_lines:
                line_text = raw_line.decode("utf-8", errors="strict")
                elements.append(
                    jcs_loads(
                        line_text,
                        max_depth=elem_max_depth,
                        max_object_members=max_object_members,
                        max_string_bytes=max_string_bytes,
                    )
                )
            if opts["sort_set"]:
                canonical_bytes = jcs_sort_set_bytes(elements)
            else:
                canonical_bytes = (
                    b"["
                    + b",".join(jcs_dumps(item).encode("utf-8") for item in elements)
                    + b"]"
                )
        else:
            if target == "-":
                raw_bytes = _read_bounded_bytes(sys.stdin.buffer, max_bytes)
            else:
                try:
                    with open(target, "rb") as f:
                        raw_bytes = _read_bounded_bytes(f, max_bytes)
                except OSError as exc:
                    sys.stderr.write(f"jcs: cannot read {target}: {exc}\n")
                    return EXIT_INPUT
            raw_text = raw_bytes.decode("utf-8", errors="strict")
            document = jcs_loads(
                raw_text,
                max_depth=max_depth,
                max_object_members=max_object_members,
                max_string_bytes=max_string_bytes,
            )
            if opts["sort_set"]:
                canonical_bytes = jcs_sort_set_bytes(document)
            else:
                canonical_bytes = jcs_dumps(document).encode("utf-8")
    except UnicodeDecodeError as exc:
        sys.stderr.write(f"jcs: invalid_utf8: {exc}\n")
        return EXIT_REFUSED
    except ValueError as exc:
        sys.stderr.write(f"jcs: {exc}\n")
        return EXIT_REFUSED

    try:
        if opts["check"]:
            if raw_bytes != canonical_bytes:
                sys.stderr.write("jcs: not_canonical: input is not canonical JCS\n")
                return EXIT_REFUSED
            return EXIT_OK
        if opts["digest"]:
            digest_hex = hashlib.sha256(canonical_bytes).hexdigest()
            sys.stdout.buffer.write(f"sha256:{digest_hex}\n".encode("ascii"))
            return EXIT_OK
        if opts["vector"]:
            digest_hex = hashlib.sha256(canonical_bytes).hexdigest()
            vector_obj = {
                "byte_length": len(canonical_bytes),
                "canonical": canonical_bytes.decode("utf-8"),
                "digest": f"sha256:{digest_hex}",
                "scheme": "jcs/rfc8785",
            }
            sys.stdout.buffer.write((jcs_dumps(vector_obj) + "\n").encode("utf-8"))
            return EXIT_OK
        sys.stdout.buffer.write(canonical_bytes)
    except OSError as exc:
        sys.stderr.write(f"jcs: output error: {exc}\n")
        return EXIT_OUTPUT
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
