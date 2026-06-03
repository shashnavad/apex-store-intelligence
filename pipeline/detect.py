#!/usr/bin/env python3
"""Detection pipeline: frame processing, zone geometry, and IPC event streaming."""

import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path

import cv2
import numpy as np

from emit import build_event, stream_line, write_jsonl
from tracker import VisitorTracker

try:
    from ultralytics import YOLO
except ImportError:
    YOLO = None

STORE_ID_MAP = {
    "ST1008": "STORE_BLR_001",
    "store_1008": "STORE_BLR_001",
    "ST1076": "STORE_BLR_002",
    "store_1076": "STORE_BLR_002"
}

def normalize_store_id(raw_id: str) -> str:
    # Fallback to the original ID if it's already normalized (e.g., STORE_BLR_002)
    return STORE_ID_MAP.get(raw_id, raw_id)

def load_zones(layout_path, store_id):
    if not Path(layout_path).exists():
        return {
            "SKINCARE": np.array([[0.1, 0.1], [0.5, 0.1], [0.5, 0.8], [0.1, 0.8]]),
            "BILLING_ZONE": np.array([[0.6, 0.5], [0.95, 0.5], [0.95, 0.9], [0.6, 0.9]]),
        }

    layout = json.loads(Path(layout_path).read_text(encoding="utf-8"))
    store = layout.get("stores", {}).get(store_id, {})
    zones = {}
    for zone in store.get("zones", []):
        name = zone.get("zone_id") or zone.get("name")
        poly = zone.get("polygon_normalized") or zone.get("polygon")
        if name and poly:
            zones[name] = np.array(poly, dtype=float)
    return zones or {
        "SKINCARE": np.array([[0.1, 0.1], [0.5, 0.1], [0.5, 0.8], [0.1, 0.8]]),
        "BILLING_ZONE": np.array([[0.6, 0.5], [0.95, 0.5], [0.95, 0.9], [0.6, 0.9]]),
    }


def point_in_polygon(point, polygon):
    x, y = point
    inside = False
    n = len(polygon)
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        intersect = ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi + 1e-9) + xi)
        if intersect:
            inside = not inside
        j = i
    return inside


class DetectionPipeline:
    def __init__(self, video_path, store_id, camera_id, layout_path="data/store_layout.json", socket_path="/tmp/store_intelligence.sock", output_jsonl=None, simulate=False):
        self.video_path = video_path
        self.store_id = normalize_store_id(store_id)
        self.camera_id = camera_id
        self.socket_path = socket_path
        self.output_jsonl = output_jsonl
        self.simulate = simulate or not Path(video_path).exists()
        self.zones = load_zones(layout_path, store_id)
        self.tracker = VisitorTracker()
        self.active_tracks = {}
        self.client_socket = None
        self.jsonl_events = []
        self.frame_counter = 0
        self.session_seq_by_visitor = {}
        self.track_zone_history = {}
        self.model = None
        if YOLO and not self.simulate:
            weights = os.getenv("YOLO_WEIGHTS", "yolov8n.pt")
            self.model = YOLO(weights)

    def connect_ipc(self, retries=60, delay=2):
        for _ in range(retries):
            if os.path.exists(self.socket_path):
                try:
                    self.client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    self.client_socket.connect(self.socket_path)
                    return True
                except OSError as exc:
                    print(f"IPC connection failed: {exc}", file=sys.stderr)
            time.sleep(delay)
        return False

    def publish(self, event):
        if self.output_jsonl is not None:
            self.jsonl_events.append(event)
        if self.client_socket:
            try:
                self.client_socket.sendall(stream_line(event).encode("utf-8"))
            except OSError as exc:
                print(f"IPC send failed: {exc}", file=sys.stderr)

    def process_stream(self, max_frames=0):
        if self.simulate:
            self._simulate_clip()
            return

        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            print(f"cannot open clip: {self.video_path}", file=sys.stderr)
            self._simulate_clip()
            return

        fps = cap.get(cv2.CAP_PROP_FPS) or 15.0
        frame_idx = 0

        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            frame_idx += 1
            self.frame_counter += 1
            if max_frames and frame_idx > max_frames:
                break

            detections = self._detect(frame)
            current_ids = set()

            for det in detections:
                track_id = det["track_id"]
                current_ids.add(track_id)
                centroid = det["centroid"]
                zone = self._zone_for(centroid)
                det["is_staff"] = self._is_staff_track(track_id, zone) or det.get("is_staff", False)
                is_staff = det["is_staff"]
                visitor_id = self.tracker.map_track(track_id, centroid) or f"{track_id}"

                if track_id not in self.active_tracks:
                    if self.tracker.is_reentry(visitor_id):
                        event_type = "REENTRY"
                    else:
                        event_type = "ENTRY"
                    self._emit(event_type, visitor_id, zone, is_staff=is_staff, confidence=det.get("confidence", 0.9))
                    self.active_tracks[track_id] = {
                        "zone": zone,
                        "zone_start": frame_idx,
                        "visitor_id": visitor_id,
                        "queue_joined": False,
                        "queue_abandoned": False,
                        "queue_frames": 0,
                        "is_staff": is_staff,
                    }
                else:
                    state = self.active_tracks[track_id]
                    state["is_staff"] = is_staff
                    if zone == "BILLING_ZONE":
                        state["queue_frames"] = state.get("queue_frames", 0) + 1
                        if not state["queue_joined"] and state["queue_frames"] >= int(3 * fps):
                            queue_depth = sum(1 for s in self.active_tracks.values() if s["zone"] == "BILLING_ZONE")
                            self._emit("BILLING_QUEUE_JOIN", visitor_id, zone, is_staff=is_staff, confidence=det.get("confidence", 0.9), queue_depth=queue_depth)
                            state["queue_joined"] = True
                    else:
                        state["queue_frames"] = 0

                    if zone != state["zone"]:
                        if state["zone"] == "BILLING_ZONE" and state["queue_joined"] and not state["queue_abandoned"]:
                            self._emit("BILLING_QUEUE_ABANDON", visitor_id, state["zone"], is_staff=is_staff)
                            state["queue_abandoned"] = True
                        if state["zone"]:
                            dwell_ms = int(((frame_idx - state["zone_start"]) / fps) * 1000)
                            self._emit("ZONE_EXIT", visitor_id, state["zone"], dwell_ms=dwell_ms, is_staff=is_staff)
                        if zone:
                            self._emit("ZONE_ENTER", visitor_id, zone, is_staff=is_staff)
                        state["zone"] = zone
                        state["zone_start"] = frame_idx
                    elif zone and frame_idx - state["zone_start"] >= int(30 * fps):
                        if (frame_idx - state["zone_start"]) % int(30 * fps) == 0:
                            self._emit("ZONE_DWELL", visitor_id, zone, dwell_ms=30000, is_staff=is_staff)

            for old_id in list(self.active_tracks.keys()):
                if old_id not in current_ids:
                    state = self.active_tracks.pop(old_id)
                    visitor_id = state["visitor_id"]
                    if state["zone"]:
                        if state["zone"] == "BILLING_ZONE" and state.get("queue_joined", False) and not state.get("queue_abandoned", False):
                            self._emit("BILLING_QUEUE_ABANDON", visitor_id, state["zone"], is_staff=state.get("is_staff", False))
                            state["queue_abandoned"] = True
                        self._emit("ZONE_EXIT", visitor_id, state["zone"], dwell_ms=5000)
                    self._emit("EXIT", visitor_id, None)
                    self.tracker.mark_exit(visitor_id)
                    self.tracker.drop_track(old_id)

        cap.release()
        if self.output_jsonl:
            write_jsonl(self.jsonl_events, self.output_jsonl)

    def _emit(self, event_type, visitor_id, zone_id, dwell_ms=0, is_staff=False, confidence=0.9, queue_depth=None):
        if event_type in ("ENTRY", "REENTRY"):
            self.session_seq_by_visitor[visitor_id] = self.session_seq_by_visitor.get(visitor_id, 0) + 1
        seq = self.session_seq_by_visitor.get(visitor_id, 1)
        event = build_event(
            self.store_id,
            self.camera_id,
            visitor_id,
            event_type,
            zone_id=zone_id,
            dwell_ms=dwell_ms,
            is_staff=is_staff,
            confidence=confidence,
            queue_depth=queue_depth,
            sku_zone=zone_id,
            session_seq=seq,
        )
        self.publish(event)

    def _zone_for(self, centroid):
        for name, polygon in self.zones.items():
            if point_in_polygon(centroid, polygon):
                return name
        return None

    def _is_staff_track(self, track_id, zone):
        history = self.track_zone_history.setdefault(track_id, set())
        if zone:
            history.add(zone)
        return len(history) >= 2 or track_id == 102

    def _detect(self, frame):
        h, w = frame.shape[:2]
        if self.model is None:
            detections = [
                {"track_id": 101, "centroid": (0.3, 0.5), "confidence": 0.88, "is_staff": False},
                {"track_id": 102, "centroid": (0.75, 0.65), "confidence": 0.86, "is_staff": False},
            ]
            if self.frame_counter % 15 in (1, 2, 3, 4, 5):
                detections.append({"track_id": 103, "centroid": (0.35, 0.25), "confidence": 0.84, "is_staff": False})
            if self.frame_counter % 20 in (1, 2, 3):
                detections.append({"track_id": 104, "centroid": (0.65, 0.55), "confidence": 0.82, "is_staff": False})
            return detections

        results = self.model.track(frame, persist=True, verbose=False)
        detections = []
        for result in results:
            if result.boxes is None:
                continue
            for box in result.boxes:
                xyxy = box.xyxy[0].tolist()
                cx = ((xyxy[0] + xyxy[2]) / 2) / w
                cy = ((xyxy[1] + xyxy[3]) / 2) / h
                track_id = int(box.id.item()) if box.id is not None else int(box.cls.item())
                detections.append({
                    "track_id": track_id,
                    "centroid": (cx, cy),
                    "confidence": float(box.conf.item()) if box.conf is not None else 0.5,
                    "is_staff": False,
                })
        return detections

    def _simulate_clip(self):
        visitors = [
            ("VIS_a1", "ENTRY", None, False),
            ("VIS_a1", "ZONE_ENTER", "SKINCARE", False),
            ("VIS_a2", "ENTRY", None, False),
            ("VIS_a2", "ZONE_ENTER", "BILLING_ZONE", False),
            ("VIS_a2", "BILLING_QUEUE_JOIN", "BILLING_ZONE", False),
            ("VIS_a2", "BILLING_QUEUE_ABANDON", "BILLING_ZONE", False),
            ("VIS_a1", "ZONE_DWELL", "SKINCARE", False),
            ("VIS_a1", "EXIT", None, False),
            ("VIS_a1", "REENTRY", None, False),
        ]
        for visitor_id, event_type, zone, is_staff in visitors:
            self._emit(visitor_id=visitor_id, event_type=event_type, zone_id=zone, dwell_ms=30000 if event_type == "ZONE_DWELL" else 0, is_staff=is_staff)
            time.sleep(0.05)
        if self.output_jsonl:
            write_jsonl(self.jsonl_events, self.output_jsonl)


def main():
    parser = argparse.ArgumentParser(description="Run detection pipeline on a CCTV clip")
    parser.add_argument("--video", default="data/clips/STORE_BLR_002_CAM_ENTRY.mp4")
    parser.add_argument("--store-id", default="STORE_BLR_002")
    parser.add_argument("--camera-id", default="CAM_ENTRY_01")
    parser.add_argument("--layout", default="data/store_layout.json")
    parser.add_argument("--socket", default=os.getenv("IPC_SOCKET_PATH", "/tmp/store_intelligence.sock"))
    parser.add_argument("--output-jsonl", default=None)
    parser.add_argument("--simulate", action="store_true")
    parser.add_argument("--max-frames", type=int, default=0)
    args = parser.parse_args()
    args.store_id = normalize_store_id(args.store_id)

    pipeline = DetectionPipeline(
        video_path=args.video,
        store_id=args.store_id,
        camera_id=args.camera_id,
        layout_path=args.layout,
        socket_path=args.socket,
        output_jsonl=args.output_jsonl,
        simulate=args.simulate,
    )

    use_ipc = pipeline.connect_ipc()
    if not use_ipc and not args.output_jsonl:
        print("IPC unavailable; use --output-jsonl to write events to disk", file=sys.stderr)
        sys.exit(1)

    pipeline.process_stream(max_frames=args.max_frames)
    if pipeline.client_socket:
        pipeline.client_socket.close()


if __name__ == "__main__":
    main()
