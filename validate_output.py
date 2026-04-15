from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

REQUIRED_FIELDS = ["date", "code", "registrations", "ftds", "state"]
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
STATE_RE = re.compile(r"^[A-Z]{2}$")


class ValidationError(RuntimeError):
    pass


def validate_record(index: int, record: Any) -> None:
    if not isinstance(record, dict):
        raise ValidationError(f"Record {index} is not an object.")

    keys = list(record.keys())
    if keys != REQUIRED_FIELDS:
        raise ValidationError(
            f"Record {index} keys were {keys}, expected {REQUIRED_FIELDS} in that exact order."
        )

    if not isinstance(record["date"], str) or not DATE_RE.match(record["date"]):
        raise ValidationError(f"Record {index} has an invalid date: {record['date']!r}")

    if not isinstance(record["code"], str) or not record["code"].strip():
        raise ValidationError(f"Record {index} has an invalid code: {record['code']!r}")

    if not isinstance(record["registrations"], int):
        raise ValidationError(
            f"Record {index} registrations must be an integer, got {type(record['registrations']).__name__}."
        )

    if not isinstance(record["ftds"], int):
        raise ValidationError(
            f"Record {index} ftds must be an integer, got {type(record['ftds']).__name__}."
        )

    if not isinstance(record["state"], str) or not STATE_RE.match(record["state"]):
        raise ValidationError(f"Record {index} has an invalid state: {record['state']!r}")


def validate_output_file(path: Path) -> int:
    if not path.exists():
        raise ValidationError(f"File not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValidationError("Top-level JSON value must be an array.")

    for index, record in enumerate(data):
        validate_record(index, record)

    return len(data)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Vault Network output.json")
    parser.add_argument("path", nargs="?", default="output.json", help="Path to output.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.path).resolve()

    try:
        count = validate_output_file(path)
    except json.JSONDecodeError as exc:
        print(f"Invalid JSON: {exc}", file=sys.stderr)
        return 1
    except ValidationError as exc:
        print(f"Validation failed: {exc}", file=sys.stderr)
        return 1

    print(f"Validation passed for {path} ({count} record(s)).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
