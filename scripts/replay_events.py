#!/usr/bin/env python3
"""Replay JSONL detection events into POST /events/ingest."""

import json
import sys
from pathlib import Path

import requests


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def main():
    api = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    files = sys.argv[2:] if len(sys.argv) > 2 else ["output/events/simulated.jsonl"]

    events = []
    for pattern in files:
        for path in Path().glob(pattern):
            if not path.is_file():
                continue
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    events.append(json.loads(line))

    for batch in chunks(events, 500):
        response = requests.post(f"{api}/events/ingest", json=batch, timeout=30)
        response.raise_for_status()
        print(response.json())


if __name__ == "__main__":
    main()
