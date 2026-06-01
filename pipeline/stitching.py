import time
import math
from datetime import datetime, timezone

class SessionStitcher:
    def __init__(self, spatial_threshold=0.15, temporal_threshold_sec=120):
        # spatial_threshold: Max normalized distance to assume it is the same individual
        # temporal_threshold_sec: Maximum time window to merge broken tracking IDs
        self.spatial_threshold = spatial_threshold
        self.temporal_threshold_sec = temporal_threshold_sec
        
        # Dead track storage: mapping stitched_id -> {last_centroid, exit_time, original_ids}
        self.historical_tracks = {}
        # Active track mapping: mapping live_yolo_id -> stitched_id
        self.live_mappings = {}

    def compute_distance(self, p1, p2):
        return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2)

    def register_frame_tracks(self, current_live_tracks):
        """
        current_live_tracks: dict of yolo_id -> centroid coordinate [x, y]
        Returns a dictionary mapping the current yolo_id to a verified stitched_id.
        """
        updated_mappings = {}
        now = time.time()

        for yolo_id, centroid in current_live_tracks.items():
            if yolo_id in self.live_mappings:
                # Track is actively moving and stable
                stitched_id = self.live_mappings[yolo_id]
                updated_mappings[yolo_id] = stitched_id
                # Update history with latest position
                if stitched_id in self.historical_tracks:
                    self.historical_tracks[stitched_id]["last_centroid"] = centroid
            else:
                # A new tracking ID has emerged. Attempt to stitch with a recently lost identity.
                matched_stitched_id = None
                best_distance = self.spatial_threshold

                for h_id, data in list(self.historical_tracks.items()):
                    time_elapsed = now - data["exit_time"]
                    
                    if time_elapsed <= self.temporal_threshold_sec:
                        dist = self.compute_distance(centroid, data["last_centroid"])
                        if dist < best_distance:
                            best_distance = dist
                            matched_stitched_id = h_id

                if matched_stitched_id:
                    # Successfully stitched fragmented track or re-entering customer
                    stitched_id = matched_stitched_id
                    self.historical_tracks[stitched_id]["original_ids"].append(yolo_id)
                else:
                    # Truly unique new customer session established
                    stitched_id = f"STITCH_{int(now * 1000)}_{yolo_id}"
                    self.historical_tracks[stitched_id] = {
                        "original_ids": [yolo_id],
                    }

                self.historical_tracks[stitched_id]["last_centroid"] = centroid
                updated_mappings[yolo_id] = stitched_id

        # Clean up old records outside the temporal threshold to free up memory
        for h_id, data in list(self.historical_tracks.items()):
            if "exit_time" in data and (now - data["exit_time"]) > self.temporal_threshold_sec:
                del self.historical_tracks[h_id]

        self.live_mappings = updated_mappings
        return self.live_mappings

    def handle_track_drop(self, dropped_yolo_id):
        """
        Signals that a track was lost by the CV engine, marking its exit timestamp.
        """
        if dropped_yolo_id in self.live_mappings:
            stitched_id = self.live_mappings[dropped_yolo_id]
            if stitched_id in self.historical_tracks:
                self.historical_tracks[stitched_id]["exit_time"] = time.time()
            del self.live_mappings[dropped_yolo_id]