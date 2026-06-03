import socket
import json
import time
import requests
import subprocess

def run_integration_chaos_harness():
    socket_path = "/tmp/store_intelligence.sock"
    api_url = "http://localhost:8080/health"
    
    print("Starting integration chaos testing harness...")

    # Step 1: Verify Go API Core is online and accessible
    try:
        response = requests.get(api_url, timeout=2)
        if response.status_code != 200:
            print(f"API Core failed basic health verification checks: {response.status_code}")
            return False
    except requests.exceptions.RequestException as err:
        print(f"Could not connect to Go core API: {err}")
        return False

    # Step 2: Simulate defensive processing by streaming malformed JSON payloads over HTTP
    print("Testing system recovery against malformed payloads...")
    ingest_url = "http://localhost:8080/events/ingest"
    try:
        # 1. Send malformed/corrupted data fragments to verify API resilience
        try:
            requests.post(ingest_url, data='{"event_id": "err_1", "store_id": "BLR_02", "visitor_id": ', headers={"Content-Type": "application/json"}, timeout=2)
            requests.post(ingest_url, data='INVALID_RAW_BYTE_STREAM_TOKEN_CORRUPT', headers={"Content-Type": "application/json"}, timeout=2)
        except requests.exceptions.RequestException:
            # Code should gracefully reject bad payloads (400 Bad Request) without crashing the server
            pass
        
        # 2. Follow up with a well-formed event to verify the system recovers and keeps running
        valid_recovery_event = {
            "event_id": "recovery_verification_02",
            "store_id": "STORE_BLR_002",
            "camera_id": "CAM_01",
            "visitor_id": "VIS_CHAOS_USER",
            "event_type": "ENTRY",
            "timestamp": "2026-06-01T12:00:00Z",
            "is_staff": False,
            "confidence": 0.99
        }
        
        # Wrap in a JSON array as expected by the Go batch endpoint signature
        response = requests.post(ingest_url, json=[valid_recovery_event], timeout=2)
        if response.status_code not in [200, 201]:
            print(f"Server rejected valid recovery payload with status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"HTTP communication testing error: {e}")
        return False
    # Step 3: Validate database persistence and metric collection
    time.sleep(1) # Allow processing time for async operations to complete
    metrics_url = "http://localhost:8080/stores/STORE_BLR_002/metrics"
    try:
        metric_res = requests.get(metrics_url, timeout=2)
        data = metric_res.json()
        
        # Verify the recovery event was safely captured despite the preceding bad data
        if data.get("unique_visitors", 0) >= 1:
            print("Payload resilience and recovery checks passed successfully.")
            return True
        else:
            print("System core dropped valid payloads sent after corrupted data.")
            return False
    except Exception as e:
        print(f"Failed to query operational status metrics from API endpoint: {e}")
        return False

if __name__ == "__main__":
    success = run_integration_chaos_harness()
    if success:
        print("Integration testing completed successfully.")
    else:
        print("Integration testing detected failures in the system configuration.")