#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${DATA_DIR:-$ROOT/data}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT/output/events}"
SIMULATE="${SIMULATE:-0}"

mkdir -p "$OUTPUT_DIR"

run_clip() {
  local video="$1"
  local store="$2"
  local camera="$3"
  local out="$OUTPUT_DIR/$(basename "$video" .mp4).jsonl"
  local args=(--video "$video" --store-id "$store" --camera-id "$camera" --layout "$DATA_DIR/store_layout.json" --output-jsonl "$out")

  if [[ "$SIMULATE" == "1" ]]; then
    args+=(--simulate)
  fi

  python3 "$ROOT/pipeline/detect.py" "${args[@]}"
}

if [[ -d "$DATA_DIR/clips" ]]; then
  for clip in "$DATA_DIR/clips"/*.mp4; do
    [[ -f "$clip" ]] || continue
    base="$(basename "$clip")"
    store="STORE_BLR_002"
    camera="CAM_ENTRY_01"
  if [[ "$base" == *BILLING* ]]; then camera="CAM_BILLING_01"; fi
  if [[ "$base" == *FLOOR* ]]; then camera="CAM_FLOOR_01"; fi
    run_clip "$clip" "$store" "$camera"
  done
else
  echo "No clips found under $DATA_DIR/clips; running simulated pipeline"
  SIMULATE=1 python3 "$ROOT/pipeline/detect.py" --simulate --output-jsonl "$OUTPUT_DIR/simulated.jsonl"
fi

echo "Events written to $OUTPUT_DIR"
echo "Replay into API with: python3 scripts/replay_events.py $OUTPUT_DIR/*.jsonl"
