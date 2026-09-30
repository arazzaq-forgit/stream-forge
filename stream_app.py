"""
stream_app.py — Stream Forge
Real-Time Truck Telemetry Streaming Pipeline

Topology:

    Kafka
      ↓
    Filter (temperature > 0)
      ↓
    Map (round temperature)
      ↓
    5-Minute Tumbling Window
      ↓
    Rolling Average per Truck
      ↓
    Live Dashboard / Prometheus

Run:
    faust -A stream_app worker -l info

Dashboard:
    http://localhost:6066/dashboard

Metrics:
    http://localhost:6066/metrics
"""

import time
from datetime import datetime, timezone

import faust
from prometheus_client import (
    Counter,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)


# ============================================================
# APP CONFIGURATION
# ============================================================

app = faust.App(
    "stream-forge",
    broker="kafka://localhost:9092",
    store="rocksdb://",
    topic_partitions=6,
)


# ============================================================
# PROMETHEUS METRICS
# ============================================================

EVENTS_PROCESSED = Counter(
    "streamforge_events_processed_total",
    "Total telemetry events processed",
)

EVENTS_FILTERED = Counter(
    "streamforge_events_filtered_total",
    "Total telemetry events rejected by the filter",
)

PROCESSING_LAG_SECONDS = Gauge(
    "streamforge_processing_lag_seconds",
    "Seconds between event timestamp and processing time",
)

ACTIVE_TRUCKS = Gauge(
    "streamforge_active_trucks",
    "Number of trucks seen by this worker",
)


# ============================================================
# EVENT SCHEMA
# ============================================================

class TruckReading(faust.Record, serializer="json"):
    """
    Incoming truck telemetry event.
    """

    truck_id: str
    temperature_c: float
    timestamp: str


# ============================================================
# KAFKA TOPIC
# ============================================================

truck_telemetry_topic = app.topic(
    "truck-telemetry",
    value_type=TruckReading,
)


# ============================================================
# CONSUMER THROUGHPUT
# ============================================================

_processed_count = 0
_counter_window_start = time.monotonic()

_active_trucks = set()
_truck_stats = {}


@app.timer(interval=5.0)
async def report_consumer_throughput():
    """
    Reports consumer-side throughput every 5 seconds.
    """

    global _processed_count
    global _counter_window_start

    now = time.monotonic()

    elapsed = now - _counter_window_start

    rate = (
        _processed_count / elapsed
        if elapsed > 0
        else 0.0
    )

    app.logger.info(
        "CONSUMER THROUGHPUT: %d messages in %.1fs = %.0f msgs/s",
        _processed_count,
        elapsed,
        rate,
    )

    _processed_count = 0
    _counter_window_start = now


# ============================================================
# 5-MINUTE WINDOW
# ============================================================

WINDOW_SIZE = 300.0


rolling_temp_table = (
    app.Table(
        "rolling-temp-by-truck",
        default=list,
        partitions=6,
    )
    .tumbling(
        WINDOW_SIZE,
        expires=WINDOW_SIZE * 2,
    )
    .relative_to_stream()
)


# ============================================================
# MAIN STREAM PROCESSING PIPELINE
# ============================================================

@app.agent(truck_telemetry_topic)
async def process_truck_readings(readings):

    global _processed_count

    async for reading in readings:

        # ----------------------------------------------------
        # Count incoming event
        # ----------------------------------------------------

        _processed_count += 1

        EVENTS_PROCESSED.inc()

        # ----------------------------------------------------
        # FILTER
        # Remove invalid / non-positive temperatures
        # ----------------------------------------------------

        if reading.temperature_c <= 0:

            EVENTS_FILTERED.inc()

            continue

        # ----------------------------------------------------
        # MAP
        # Normalize temperature
        # ----------------------------------------------------

        mapped_temp = round(
            reading.temperature_c,
            1,
        )

        # ----------------------------------------------------
        # PROCESSING LAG
        # ----------------------------------------------------

        try:

            event_time = datetime.fromisoformat(
                reading.timestamp
            )

            lag = (
                datetime.now(timezone.utc)
                - event_time
            ).total_seconds()

            PROCESSING_LAG_SECONDS.set(
                max(0.0, lag)
            )

        except (ValueError, TypeError):

            pass

        # ----------------------------------------------------
        # TRACK ACTIVE TRUCK
        # ----------------------------------------------------

        _active_trucks.add(
            reading.truck_id
        )

        ACTIVE_TRUCKS.set(
            len(_active_trucks)
        )

        # ----------------------------------------------------
        # WINDOWED AGGREGATION
        # ----------------------------------------------------

        current_bucket = (
            rolling_temp_table[
                reading.truck_id
            ].value()
        )

        current_bucket.append(
            mapped_temp
        )

        rolling_temp_table[
            reading.truck_id
        ] = current_bucket

        # ----------------------------------------------------
        # ROLLING AVERAGE
        # ----------------------------------------------------

        rolling_avg = (
            sum(current_bucket)
            / len(current_bucket)
        )

        # ----------------------------------------------------
        # DEBUG LOG
        # ----------------------------------------------------

        app.logger.debug(
            "truck=%s latest=%.1f°C "
            "rolling_avg(5min)=%.2f°C samples=%d",
            reading.truck_id,
            mapped_temp,
            rolling_avg,
            len(current_bucket),
        )

        _truck_stats[reading.truck_id] = {
            "truck_id": reading.truck_id,
            "samples": len(current_bucket),
            "rolling_avg": round(rolling_avg, 2),
            "latest": mapped_temp,
        }


# ============================================================
# SINGLE TRUCK API
# ============================================================

@app.page("/truck/{truck_id}/")
async def get_truck_average(
    web,
    request,
    truck_id: str,
):

    bucket = (
        rolling_temp_table[
            truck_id
        ].now()
    )

    if not bucket:

        return web.json(
            {
                "truck_id": truck_id,
                "samples": 0,
                "rolling_avg": None,
            }
        )

    return web.json(
        {
            "truck_id": truck_id,
            "samples": len(bucket),
            "rolling_avg": round(
                sum(bucket)
                / len(bucket),
                2,
            ),
        }
    )


# ============================================================
# PROMETHEUS ENDPOINT
# ============================================================

@app.page("/metrics")
async def metrics(web, request):

    return web.text(
        generate_latest().decode("utf-8"),
        content_type="text/plain",
    )


# ============================================================
# DASHBOARD DATA API
# ============================================================

@app.page("/dashboard-data")
async def dashboard_data(
    web,
    request,
):

    trucks = []

    for truck_id in sorted(
        _active_trucks
    )[:50]:

        bucket = (
            rolling_temp_table[
                truck_id
            ].now()
        )

        if bucket:

            trucks.append(
                {
                    "truck_id": truck_id,
                    "samples": len(bucket),
                    "rolling_avg": round(
                        sum(bucket)
                        / len(bucket),
                        2,
                    ),
                    "latest": bucket[-1],
                }
            )

    if not trucks:
        trucks = list(_truck_stats.values())[:50]

    trucks.sort(
        key=lambda truck: truck["rolling_avg"],
        reverse=True,
    )

    return web.json(
        {
            "active_trucks": len(
                _active_trucks
            ),

            "events_processed":
                EVENTS_PROCESSED
                ._value
                .get(),

            "events_filtered":
                EVENTS_FILTERED
                ._value
                .get(),

            "processing_lag_seconds":
                round(
                    PROCESSING_LAG_SECONDS
                    ._value
                    .get(),
                    2,
                ),

            "trucks": trucks,
        }
    )


# ============================================================
# DASHBOARD
# ============================================================

_DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Stream Forge — Real-Time Dashboard</title>
<style>
* { box-sizing: border-box; }
body { margin: 0; font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: radial-gradient(circle at top left, #172554, #0f172a 45%, #020617); color: #f8fafc; min-height: 100vh; }
.header { display: flex; justify-content: space-between; align-items: center; padding: 24px 40px; border-bottom: 1px solid rgba(255,255,255,0.08); background: rgba(15,23,42,0.65); backdrop-filter: blur(10px); }
.brand { display: flex; align-items: center; gap: 14px; }
.logo { width: 46px; height: 46px; display: flex; align-items: center; justify-content: center; border-radius: 12px; background: linear-gradient(135deg, #2563eb, #7c3aed); font-size: 24px; }
.title { font-size: 21px; font-weight: 700; letter-spacing: -0.4px; }
.subtitle { color: #94a3b8; font-size: 12px; margin-top: 3px; }
.status { display: flex; align-items: center; gap: 8px; padding: 8px 14px; border-radius: 999px; background: rgba(34,197,94,0.10); border: 1px solid rgba(34,197,94,0.25); color: #4ade80; font-size: 13px; font-weight: 600; }
.status-dot { width: 8px; height: 8px; border-radius: 50%; background: #22c55e; box-shadow: 0 0 10px rgba(34,197,94,0.8); animation: pulse 1.5s infinite; }
@keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.35; } 100% { opacity: 1; } }
.container { max-width: 1400px; margin: auto; padding: 32px 40px; }
.pipeline-section { margin-bottom: 32px; }
.section-title { font-size: 13px; text-transform: uppercase; letter-spacing: 0.08em; color: #64748b; margin-bottom: 14px; }
.pipeline { display: flex; align-items: center; justify-content: center; gap: 12px; padding: 25px; border-radius: 18px; background: rgba(15,23,42,0.75); border: 1px solid rgba(255,255,255,0.07); overflow-x: auto; }
.node { min-width: 155px; padding: 18px; text-align: center; border-radius: 14px; background: linear-gradient(145deg, rgba(30,41,59,0.95), rgba(15,23,42,0.95)); border: 1px solid rgba(96,165,250,0.22); box-shadow: 0 8px 25px rgba(0,0,0,0.2); }
.node-icon { font-size: 25px; margin-bottom: 8px; }
.node-title { font-weight: 700; font-size: 14px; }
.node-desc { color: #94a3b8; font-size: 11px; margin-top: 4px; }
.arrow { color: #60a5fa; font-size: 25px; flex-shrink: 0; }
.metrics { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }
.card { padding: 20px; border-radius: 16px; background: rgba(15,23,42,0.75); border: 1px solid rgba(255,255,255,0.07); }
.card-label { color: #94a3b8; font-size: 12px; text-transform: uppercase; letter-spacing: 0.06em; }
.card-value { font-size: 30px; font-weight: 750; margin-top: 9px; color: #f8fafc; }
.card-unit { color: #64748b; font-size: 12px; }
.table-card { border-radius: 16px; overflow: hidden; background: rgba(15,23,42,0.8); border: 1px solid rgba(255,255,255,0.07); }
.table-header { display: flex; justify-content: space-between; align-items: center; padding: 20px 22px; border-bottom: 1px solid rgba(255,255,255,0.06); }
.table-title { font-size: 16px; font-weight: 700; }
.live-text { color: #4ade80; font-size: 12px; }
table { width: 100%; border-collapse: collapse; }
th { text-align: left; padding: 13px 20px; color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; background: rgba(30,41,59,0.35); }
td { padding: 15px 20px; border-top: 1px solid rgba(255,255,255,0.05); font-size: 13px; }
.truck-id { font-weight: 600; color: #e2e8f0; }
.temperature { font-weight: 700; }
.average { font-weight: 700; color: #4ade80; }
.sample { color: #94a3b8; }
.empty { text-align: center; padding: 50px; color: #64748b; }
.footer { text-align: center; color: #475569; font-size: 11px; padding: 25px 0 5px; }
@media (max-width: 900px) { .metrics { grid-template-columns: repeat(2, 1fr); } .header { padding: 20px; } .container { padding: 25px 20px; } }
</style>
</head>
<body>
<header class="header">
    <div class="brand">
        <div class="logo">🚚</div>
        <div>
            <div class="title">Stream Forge</div>
            <div class="subtitle">Real-Time Truck Telemetry Platform</div>
        </div>
    </div>
    <div class="status">
        <span class="status-dot"></span>
        STREAMING LIVE
    </div>
</header>
<main class="container">
<section class="pipeline-section">
    <div class="section-title">Streaming Pipeline</div>
    <div class="pipeline">
        <div class="node"><div class="node-icon">📡</div><div class="node-title">Kafka</div><div class="node-desc">Truck Telemetry</div></div>
        <div class="arrow">→</div>
        <div class="node"><div class="node-icon">🔍</div><div class="node-title">Filter</div><div class="node-desc">Temperature &gt; 0</div></div>
        <div class="arrow">→</div>
        <div class="node"><div class="node-icon">⚙️</div><div class="node-title">Map</div><div class="node-desc">Normalize Data</div></div>
        <div class="arrow">→</div>
        <div class="node"><div class="node-icon">⏱️</div><div class="node-title">5-Min Window</div><div class="node-desc">Tumbling Window</div></div>
        <div class="arrow">→</div>
        <div class="node"><div class="node-icon">📊</div><div class="node-title">Rolling Average</div><div class="node-desc">Per Truck</div></div>
    </div>
</section>
<section class="metrics">
    <div class="card"><div class="card-label">Active Trucks</div><div class="card-value" id="m-active">—</div><div class="card-unit">vehicles detected</div></div>
    <div class="card"><div class="card-label">Events Processed</div><div class="card-value" id="m-processed">—</div><div class="card-unit">telemetry events</div></div>
    <div class="card"><div class="card-label">Events Filtered</div><div class="card-value" id="m-filtered">—</div><div class="card-unit">invalid readings</div></div>
    <div class="card"><div class="card-label">Processing Lag</div><div class="card-value" id="m-lag">—</div><div class="card-unit">seconds</div></div>
</section>
<section class="table-card">
    <div class="table-header">
        <div class="table-title">Live Truck Telemetry</div>
        <div class="live-text" id="last-update">Updating...</div>
    </div>
    <table>
        <thead>
            <tr><th>Truck ID</th><th>Latest Temperature</th><th>5-Min Rolling Average</th><th>Samples</th></tr>
        </thead>
        <tbody id="truck-rows">
            <tr><td colspan="4" class="empty">Connecting to Stream Forge...</td></tr>
        </tbody>
    </table>
</section>
<div class="footer">Stream Forge · Kafka + Faust + RocksDB + Prometheus</div>
</main>
<script>
async function refreshDashboard() {
    try {
        const response = await fetch("/dashboard-data");
        if (!response.ok) {
            throw new Error("Dashboard API returned " + response.status);
        }
        const data = await response.json();

        document.getElementById("m-active").textContent = data.active_trucks;
        document.getElementById("m-processed").textContent = Number(data.events_processed).toLocaleString();
        document.getElementById("m-filtered").textContent = Number(data.events_filtered).toLocaleString();
        document.getElementById("m-lag").textContent = data.processing_lag_seconds;
        document.getElementById("last-update").textContent = "Updated " + new Date().toLocaleTimeString();

        const tbody = document.getElementById("truck-rows");

        if (!data.trucks || data.trucks.length === 0) {
            tbody.innerHTML = `<tr><td colspan="4" class="empty">No truck telemetry yet. Start the Kafka producer.</td></tr>`;
            return;
        }

        tbody.innerHTML = data.trucks.map(truck => `
            <tr>
                <td class="truck-id">🚛 ${truck.truck_id}</td>
                <td class="temperature">${Number(truck.latest).toFixed(1)} °C</td>
                <td class="average">${Number(truck.rolling_avg).toFixed(2)} °C</td>
                <td class="sample">${truck.samples}</td>
            </tr>
        `).join("");
    }
    catch (error) {
        console.error("Dashboard error:", error);
        document.getElementById("last-update").textContent = "Connection error";
    }
}
refreshDashboard();
setInterval(refreshDashboard, 2000);
</script>
</body>
</html>
"""


# ============================================================
# DASHBOARD ROUTE
# ============================================================

@app.page("/dashboard")
async def dashboard(
    web,
    request,
):

    return web.html(
        _DASHBOARD_HTML
    )


# ============================================================
# APPLICATION ENTRY POINT
# ============================================================

if __name__ == "__main__":

    app.main()