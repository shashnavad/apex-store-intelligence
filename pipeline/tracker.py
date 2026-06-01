"""Cross-camera session stitching and re-entry detection."""

from stitching import SessionStitcher


class VisitorTracker:
    def __init__(self):
        self.stitcher = SessionStitcher()
        self.closed_visitors = set()

    def map_track(self, yolo_id, centroid):
        return self.stitcher.register_frame_tracks({yolo_id: centroid}).get(yolo_id)

    def drop_track(self, yolo_id):
        self.stitcher.handle_track_drop(yolo_id)

    def is_reentry(self, stitched_id):
        return stitched_id in self.closed_visitors

    def mark_exit(self, stitched_id):
        self.closed_visitors.add(stitched_id)
