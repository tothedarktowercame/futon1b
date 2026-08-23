#!/usr/bin/env python3
"""Append one futon1b generated-class/metaspace sample to a dated JSONL log."""

import argparse
import datetime as dt
import json
from pathlib import Path
import re
import subprocess
import sys
import urllib.request


DEFAULT_OUTPUT_DIR = Path.home() / "code/storage/futon1b/metaspace"
DEFAULT_HEALTH_URL = "http://127.0.0.1:7073/health"
METASPACE_ALERT_MB = 1024
MONOTONIC_WINDOW_HOURS = 6


def run(*args: str) -> str:
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout


def service_pid(unit: str) -> int:
    value = run("systemctl", "--user", "show", unit, "--property=MainPID", "--value").strip()
    pid = int(value)
    if pid <= 0:
        raise RuntimeError(f"{unit} has no live MainPID")
    return pid


def dynamic_classloaders(pid: int) -> tuple[int, int, str]:
    matches = []
    for line in run("jcmd", str(pid), "GC.class_histogram").splitlines():
        if "DynamicClassLoader" in line:
            match = re.match(r"\s*\d+:\s+(\d+)\s+(\d+)\s+(.+?)\s*$", line)
            if match:
                matches.append((int(match.group(1)), int(match.group(2)), match.group(3)))
    if not matches:
        raise RuntimeError("jcmd histogram contained no DynamicClassLoader row")
    return (sum(row[0] for row in matches), sum(row[1] for row in matches), ";".join(row[2] for row in matches))


def cheap_health(url: str) -> tuple[int | None, str | None]:
    if "?" in url:
        raise ValueError("health URL must be the cheap endpoint without a query string")
    with urllib.request.urlopen(url, timeout=10) as response:
        body = response.read().decode("utf-8")
    match = re.search(r":metaspace-used-mb\s+(\d+)", body)
    return (int(match.group(1)) if match else None, None if match else "field-absent")


def prior_rows(output_dir: Path, pid: int) -> list[dict]:
    rows = []
    for path in sorted(output_dir.glob("metaspace-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
                timestamp = dt.datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
                if row.get("pid") == pid:
                    rows.append(row)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--unit", default="futon1b-zone.service")
    parser.add_argument("--health-url", default=DEFAULT_HEALTH_URL)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    now = dt.datetime.now(dt.timezone.utc)
    timestamp = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    pid = service_pid(args.unit)
    count, bytes_used, class_names = dynamic_classloaders(pid)
    metaspace_mb, health_error = cheap_health(args.health_url)
    previous = prior_rows(args.output_dir, pid)
    cutoff = now - dt.timedelta(hours=MONOTONIC_WINDOW_HOURS)
    before = [row for row in previous
              if dt.datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")) <= cutoff]
    after = [row for row in previous
             if dt.datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")) > cutoff]
    series = (before[-1:] + after
              + [{"timestamp": timestamp, "metaspace-used-mb": metaspace_mb}])
    values = [row.get("metaspace-used-mb") for row in series]
    timestamps = [dt.datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")) for row in series]
    monotonic_6h = (
        len(values) >= 2
        and all(value is not None for value in values)
        and timestamps[-1] - timestamps[0] >= dt.timedelta(hours=MONOTONIC_WINDOW_HOURS)
        and values[-1] > values[0]
        and all(left <= right for left, right in zip(values, values[1:]))
    )

    last_pid = None
    # Find the latest row regardless of PID without interpreting across restarts.
    for path in sorted(args.output_dir.glob("metaspace-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                last_pid = json.loads(line).get("pid", last_pid)
            except json.JSONDecodeError:
                pass

    row = {
        "timestamp": timestamp,
        "pid": pid,
        "pid-changed": last_pid is not None and last_pid != pid,
        "dynamic-classloader-instances": count,
        "dynamic-classloader-bytes": bytes_used,
        "dynamic-classloader-classes": class_names,
        "metaspace-used-mb": metaspace_mb,
        "health-error": health_error,
        "alerts": {
            "metaspace-over-1gb": metaspace_mb is not None and metaspace_mb > METASPACE_ALERT_MB,
            "metaspace-monotonic-6h": monotonic_6h,
        },
        "thresholds": {"metaspace-mb": METASPACE_ALERT_MB, "monotonic-hours": MONOTONIC_WINDOW_HOURS},
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / f"metaspace-{now.date().isoformat()}.jsonl"
    with output.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    print(json.dumps(row, sort_keys=True, separators=(",", ":")))
    if any(row["alerts"].values()):
        print(f"futon1b metaspace vitality alert: {row['alerts']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
