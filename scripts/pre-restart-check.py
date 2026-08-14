#!/usr/bin/env python3
"""Fail closed when the running XTDB node has too much byte-offset backlog."""

import argparse
import json
import sys
import urllib.request
from pathlib import Path


DEFAULT_MAX_BACKLOG_BYTES = 32 * 1024 * 1024


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", type=Path, default=Path("migration-store-21"))
    parser.add_argument("--url", default="http://127.0.0.1:7073")
    parser.add_argument("--max-backlog-bytes", type=int,
                        default=DEFAULT_MAX_BACKLOG_BYTES)
    args = parser.parse_args()

    log_path = args.store / "log" / "LOG"
    try:
        log_size = log_path.stat().st_size
        request = urllib.request.Request(
            args.url.rstrip("/") + "/api/alpha/restart-readiness",
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            status = json.load(response)
    except Exception as exc:
        print(f"restart_safe=false reason=status-unavailable error={exc}",
              file=sys.stderr)
        return 2

    submitted = int(status["latest-submitted-byte-offset"])
    processed = int(status["latest-processed-byte-offset"])
    submitted_gap = max(0, submitted - processed)
    disk_gap = max(0, log_size - processed)
    judged_gap = max(submitted_gap, disk_gap)
    safe = judged_gap <= args.max_backlog_bytes

    print(f"log_size_bytes={log_size}")
    print(f"latest_submitted_byte_offset={submitted}")
    print(f"latest_processed_byte_offset={processed}")
    print(f"submitted_minus_processed_bytes={submitted_gap}")
    print(f"log_size_minus_processed_bytes={disk_gap}")
    print(f"maximum_safe_backlog_bytes={args.max_backlog_bytes}")
    print(f"restart_safe={'true' if safe else 'false'}")
    return 0 if safe else 1


if __name__ == "__main__":
    raise SystemExit(main())
