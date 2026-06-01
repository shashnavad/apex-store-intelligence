#!/usr/bin/env python3
"""Terminal dashboard for live metrics (Part E bonus)."""

import time

import requests

STORE_ID = "STORE_BLR_002"
API = "http://localhost:8080"


def main():
    while True:
        metrics = requests.get(f"{API}/stores/{STORE_ID}/metrics", timeout=5).json()
        health = requests.get(f"{API}/health", timeout=5).json()
        print(
            f"visitors={metrics.get('unique_visitors')} "
            f"conversion={metrics.get('conversion_rate'):.2%} "
            f"queue={metrics.get('queue_depth')} "
            f"status={health.get('status')}"
        )
        time.sleep(2)


if __name__ == "__main__":
    main()
