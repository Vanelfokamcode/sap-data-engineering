import json
import time
from pathlib import Path
from prometheus_client import start_http_server, Gauge, Counter

METRICS_FILE = Path(__file__).parent / "pipeline_metrics.json"

asset_duration = Gauge("sap_asset_duration_seconds", "Duree execution asset SAP", ["asset_name"])
asset_rows = Gauge("sap_asset_rows_extracted", "Rows extraites par asset SAP", ["asset_name"])
asset_status = Gauge("sap_asset_last_run_success", "1 si succes 0 sinon", ["asset_name"])
pipeline_runs_total = Counter("sap_pipeline_runs_total", "Total runs pipeline SAP")

def update_metrics():
    if not METRICS_FILE.exists():
        return
    try:
        data = json.loads(METRICS_FILE.read_text())
        for asset, info in data.get("assets", {}).items():
            asset_duration.labels(asset_name=asset).set(info.get("duration_seconds", 0))
            asset_rows.labels(asset_name=asset).set(info.get("rows_extracted", 0))
            asset_status.labels(asset_name=asset).set(1 if info.get("success") else 0)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    start_http_server(8000)
    print("Metrics exporter running on :8000/metrics")
    while True:
        update_metrics()
        time.sleep(10)
