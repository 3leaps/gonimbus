#!/usr/bin/env python3
"""Portable RFC3339 instant helper for contract: agent-wait/v0,
contract: service-job/v0, contract: durable-record, and contract: data-acquisition/v0.

Standard-library only. No GNU or BSD date(1). The helper is the
fail-closed grammar for security-sensitive ordering: it does not
delegate acceptance to datetime.fromisoformat.

Accepted profile (RFC 3339 §5.6 date-time):
  YYYY-MM-DD<T|t>hh:mm:ss[.fraction](Z|z|±hh:mm)

Rejected ISO 8601 forms that are not this profile include basic
date/time, week dates, ordinal dates, missing seconds, a space
separator, and numeric offsets without a colon.

Leap-second posture: second 60 is rejected. The helper does not
clamp 23:59:60 to 23:59:59 or roll it into the next day. A leap
second is unparseable and fails closed.

Fractional-second precision posture:
  - Python's ``datetime`` stores microsecond resolution (6 fractional digits).
    By default, ``parse_rfc3339(text)`` truncates digits beyond the 6th place
    when constructing ``datetime.microsecond``. Callers that enforce a strict
    fractional-digit ceiling or exact microsecond representability MUST pass
    ``max_fraction_digits=...`` or ``reject_submicrosecond_nonzero=True``.
  - ``compare(left, right)`` compares UTC instants at full arbitrary decimal
    fraction precision (whole-second UTC instant first, then normalized
    fractional digits with trailing zeros stripped) so sub-microsecond
    instants never falsely compare equal.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timedelta, timezone

# Extended RFC 3339 date-time only (ASCII digits 0-9 only). Seconds are
# required. Numeric offsets must include a colon. Fraction uses a period,
# not a comma.
_RFC3339 = re.compile(
    r"^(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})"
    r"[Tt]"
    r"(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})"
    r"(?P<frac>\.[0-9]+)?"
    r"(?:(?P<zulu>[Zz])|(?P<sign>[+-])(?P<off_h>[0-9]{2}):(?P<off_m>[0-9]{2}))$",
    re.ASCII,
)


def _parse_rfc3339_parts(
    text: str,
    *,
    max_fraction_digits: int | None = None,
    reject_submicrosecond_nonzero: bool = False,
) -> tuple[datetime, str]:
    if max_fraction_digits is not None and max_fraction_digits < 0:
        raise ValueError("max_fraction_digits must be >= 0")
    if not isinstance(text, str) or not text.strip() or text != text.strip():
        raise ValueError("timestamp must be a non-empty RFC3339 string")
    match = _RFC3339.fullmatch(text)
    if match is None:
        raise ValueError(f"unparseable RFC3339 timestamp: {text!r}")

    year = int(match.group("year"))
    month = int(match.group("month"))
    day = int(match.group("day"))
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    second = int(match.group("second"))
    if not (1 <= year <= 9999) or hour > 23 or minute > 59:
        raise ValueError(f"unparseable RFC3339 timestamp: {text!r}")
    if second == 60:
        raise ValueError(f"leap second is not admitted: {text!r}")
    if second > 60:
        raise ValueError(f"unparseable RFC3339 timestamp: {text!r}")

    microsecond = 0
    raw_frac = ""
    frac = match.group("frac")
    if frac is not None:
        raw_frac = frac[1:]
        if max_fraction_digits is not None and len(raw_frac) > max_fraction_digits:
            raise ValueError(
                f"fractional seconds exceed {max_fraction_digits} digits: {text!r}"
            )
        if (
            reject_submicrosecond_nonzero
            and len(raw_frac) > 6
            and raw_frac[6:].strip("0")
        ):
            raise ValueError(
                f"precision beyond microseconds is not representable: {text!r}"
            )
        digits = (raw_frac + "000000")[:6]
        microsecond = int(digits)

    if match.group("zulu") is not None:
        tzinfo = timezone.utc
    else:
        off_h = int(match.group("off_h"))
        off_m = int(match.group("off_m"))
        if off_h > 23 or off_m > 59:
            raise ValueError(f"unparseable RFC3339 timestamp: {text!r}")
        offset = timedelta(hours=off_h, minutes=off_m)
        if match.group("sign") == "-":
            offset = -offset
        tzinfo = timezone(offset)

    try:
        parsed = datetime(
            year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo
        )
        return parsed.astimezone(timezone.utc), raw_frac.rstrip("0")
    except (ValueError, OverflowError) as exc:
        raise ValueError(f"unparseable RFC3339 timestamp: {text!r}") from exc


def parse_rfc3339(
    text: str,
    *,
    max_fraction_digits: int | None = None,
    reject_submicrosecond_nonzero: bool = False,
) -> datetime:
    """Parse an RFC 3339 date-time string into a UTC ``datetime``.

    Note: ``datetime`` carries microsecond precision (6 fractional digits).
    Pass ``max_fraction_digits`` to bound raw fractional length or
    ``reject_submicrosecond_nonzero=True`` to refuse non-zero digits past the
    6th fractional place rather than truncating them.
    """
    utc_dt, _ = _parse_rfc3339_parts(
        text,
        max_fraction_digits=max_fraction_digits,
        reject_submicrosecond_nonzero=reject_submicrosecond_nonzero,
    )
    return utc_dt


def epoch_seconds(text: str) -> int:
    return int(parse_rfc3339(text).timestamp())


def compare(left: str, right: str) -> int:
    a_dt, a_frac = _parse_rfc3339_parts(left)
    b_dt, b_frac = _parse_rfc3339_parts(right)
    a_sec = a_dt.replace(microsecond=0)
    b_sec = b_dt.replace(microsecond=0)
    if a_sec < b_sec:
        return -1
    if a_sec > b_sec:
        return 1
    if a_frac < b_frac:
        return -1
    if a_frac > b_frac:
        return 1
    return 0


def self_test() -> None:
    failures: list[str] = []

    def expect_reject(label: str, text: str, **kwargs: object) -> None:
        try:
            parse_rfc3339(text, **kwargs)  # type: ignore[arg-type]
        except ValueError:
            return
        failures.append(f"{label}: expected reject of {text!r}")

    def expect_accept(label: str, text: str, **kwargs: object) -> None:
        try:
            parse_rfc3339(text, **kwargs)  # type: ignore[arg-type]
        except ValueError as exc:
            failures.append(f"{label}: expected accept of {text!r}: {exc}")

    expect_reject("empty", "")
    expect_reject("whitespace", "   ")
    expect_reject("garbage", "not-a-timestamp")
    expect_reject("date-only", "2026-08-15")
    expect_reject("naive", "2026-08-15T17:00:00")
    expect_reject("space-separator", "2026-08-15 17:00:00Z")
    expect_reject("basic-date-time", "20260815T170000Z")
    expect_reject("week-date", "2026-W33-6T17:00:00Z")
    expect_reject("ordinal-date", "2026-227T17:00:00Z")
    expect_reject("missing-seconds", "2026-08-15T17:00Z")
    expect_reject("offset-without-colon", "2026-08-15T17:00:00+0000")
    expect_reject("comma-fraction", "2026-08-15T17:00:00,123Z")
    expect_reject("invalid-calendar", "2026-02-30T17:00:00Z")
    expect_reject("hour-24", "2026-08-15T24:00:00Z")
    expect_reject("leap-second-utc", "2016-12-31T23:59:60Z")
    expect_reject("leap-second-offset", "2016-12-31T23:59:60+00:00")
    expect_reject("year-0000", "0000-01-01T00:00:00Z")
    expect_reject("year-0001-utc-underflow", "0001-01-01T00:00:00+01:00")
    expect_reject("year-9999-utc-overflow", "9999-12-31T23:59:59-01:00")
    expect_reject("non-ascii-year", "\uff12\uff10\uff12\uff16-08-15T17:00:00Z")
    expect_reject("non-ascii-hour", "2026-08-15T\uff11\uff17:00:00Z")
    expect_reject("non-ascii-fraction", "2026-08-15T17:00:00.\uff11\uff12\uff13Z")
    expect_reject("non-ascii-offset", "2026-08-15T17:00:00+\uff10\uff10:00")

    expect_reject(
        "max-fraction-0",
        "2026-08-15T17:00:00.1Z",
        max_fraction_digits=0,
    )
    expect_reject(
        "max-fraction-6-exceeded",
        "2026-08-15T17:00:00.1234560Z",
        max_fraction_digits=6,
    )
    expect_reject(
        "submicrosecond-nonzero-7",
        "2026-08-15T17:00:00.1234567Z",
        reject_submicrosecond_nonzero=True,
    )
    expect_reject(
        "submicrosecond-nonzero-9",
        "2026-08-15T17:00:00.000000001Z",
        reject_submicrosecond_nonzero=True,
    )

    expect_accept("zulu", "2026-08-15T17:00:00Z")
    expect_accept("year-0001-min", "0001-01-01T00:00:00Z")
    expect_accept("year-9999-max", "9999-12-31T23:59:59.999999Z")
    expect_accept("lowercase", "2026-08-15t17:00:00z")
    expect_accept("numeric-offset", "2026-08-15T17:00:00+00:00")
    expect_accept("unknown-local-offset-minus-zero", "2026-08-15T17:00:00-00:00")
    expect_accept("negative-offset", "2026-08-15T13:00:00-04:00")
    expect_accept("fractional-seconds", "2026-08-15T17:00:00.123Z")
    expect_accept(
        "max-fraction-6-exact",
        "2026-08-15T17:00:00.123456Z",
        max_fraction_digits=6,
    )
    expect_accept(
        "submicrosecond-trailing-zeros",
        "2026-08-15T17:00:00.123456000Z",
        reject_submicrosecond_nonzero=True,
    )

    if epoch_seconds("1970-01-01T00:00:00Z") != 0:
        failures.append("epoch 1970-01-01T00:00:00Z must be 0")
    if epoch_seconds("1970-01-01T00:00:01Z") != 1:
        failures.append("epoch 1970-01-01T00:00:01Z must be 1")
    if epoch_seconds("1970-01-01T00:00:00+00:00") != 0:
        failures.append("epoch +00:00 must match Z")
    if epoch_seconds("2026-08-15T17:00:00.123Z") != epoch_seconds(
        "2026-08-15T17:00:00Z"
    ):
        failures.append("fractional seconds must not change integer epoch")

    if compare("1970-01-01T00:00:00Z", "1970-01-01T00:00:00+00:00") != 0:
        failures.append("equality: Z vs +00:00")
    if compare("1970-01-01T01:00:00+01:00", "1970-01-01T00:00:00Z") != 0:
        failures.append("equality: +01:00 vs Z")
    if compare("2026-08-15T17:00:00+00:00", "2026-08-15T17:00:00Z") != 0:
        failures.append("equality: offset-equivalent +00:00 vs Z")
    if compare("1970-01-01T00:00:00Z", "1970-01-01T00:00:01Z") != -1:
        failures.append("before: 00:00:00Z < 00:00:01Z")
    if compare("1970-01-01T00:00:01Z", "1970-01-01T00:00:00Z") != 1:
        failures.append("after: 00:00:01Z > 00:00:00Z")
    if compare("2026-08-15T17:00:00Z", "2026-08-15T17:00:00Z") != 0:
        failures.append("equality: identical instants")
    if compare("2026-08-15T17:00:00.100Z", "2026-08-15T17:00:00Z") != 1:
        failures.append("after: fractional second is later")
    if compare("2026-08-15T16:00:10Z", "2026-08-15T17:00:00+01:00") != 1:
        failures.append("after: later instant despite earlier lexical string")
    if compare("2026-08-15T17:00:00.0000001Z", "2026-08-15T17:00:00.0000002Z") != -1:
        failures.append("before: sub-microsecond ordering .0000001Z < .0000002Z")
    if compare("2026-08-15T17:00:00.123456000Z", "2026-08-15T17:00:00.123456Z") != 0:
        failures.append("equality: trailing sub-microsecond zeros")

    if failures:
        raise ValueError("; ".join(failures))


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "--self-test":
        try:
            self_test()
        except ValueError as exc:
            sys.stderr.write(f"rfc3339-instant self-test failed: {exc}\n")
            return 1
        return 0
    if len(argv) == 3 and argv[1] == "--epoch":
        try:
            sys.stdout.write(f"{epoch_seconds(argv[2])}\n")
        except ValueError as exc:
            sys.stderr.write(f"rfc3339-instant: {exc}\n")
            return 1
        return 0
    if len(argv) == 4 and argv[1] == "--compare":
        try:
            sys.stdout.write(f"{compare(argv[2], argv[3])}\n")
        except ValueError as exc:
            sys.stderr.write(f"rfc3339-instant: {exc}\n")
            return 1
        return 0
    sys.stderr.write(
        "usage: rfc3339-instant.py --epoch TIMESTAMP\n"
        "       rfc3339-instant.py --compare A B\n"
        "       rfc3339-instant.py --self-test\n"
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
