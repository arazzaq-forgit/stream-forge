""""
stream_app.py — Week 2, Project 2 "Stream Forge"

Faust stream topology:

    Consume (truck-telemetry) -> Filter (temp > 0) -> Map (round temp)
        -> 5-minute tumbling window -> rolling average per truck

Faust gives us a Kafka Streams-like API natively in Python: it manages its
own consumer group, maintains local state in RocksDB-backed tables, and
handles partition rebalancing for us (this is what Week 3's chaos test will
exercise). For now, we build the happy-path topology and windowing logic.

Run with:
    faust -A stream_app worker -l info

This starts a single worker. To simulate multiple parallel workers (as in
the spec's "20 parallel Python worker nodes"), run this at same command in
several terminals — Faust's consumer group will automatically split the
topic's partitions across them.
"""

import time

import faust

# --- App & topic setup -------------------------------------------------

app = faust.App(
    "stream-forge",
    broker="kafka://localhost:9092",
    # RocksDB-backed table storage. This is what survives a worker crash
    # and lets Week 3's chaos test recover state instead of losing it.
    store="rocksdb://",
    topic_partitions=6,  # matches the topic we created in Week 1
)


class TruckReading(faust.Record, serializer="json"):
    """Matches the JSON shape emitted by Week 1's producer.py"""
    truck_id: str
    temperature_c: float
    timestamp: str  # ISO 8601 string; parsed to datetime where needed


truck_telemetry_topic = app.topic("truck-telemetry", value_type=TruckReading)


# --- Consumer-side throughput measurement (Week 3 audit) ---------------
#
# The producer-side benchmark (benchmark_producer.py) only proves how fast
# Kafka can ingest writes. This counter measures the other half: how fast
# THIS worker can actually pull messages off the topic and run them
# through filter -> map -> windowed aggregation. On a single machine, this
# number will typically be meaningfully lower than raw producer throughput
# — that gap IS the interesting result for the audit.

_processed_count = 0
_counter_window_start = time.monotonic()


@app.timer(interval=5.0)
async def report_consumer_throughput():
    """Runs every 5 seconds regardless of message flow; prints the
    consumer-side processing rate over that window."""
    global _processed_count, _counter_window_start
    now = time.monotonic()
    elapsed = now - _counter_window_start
    rate = _processed_count / elapsed if elapsed > 0 else 0.0
    app.logger.info(
        "CONSUMER THROUGHPUT: %d messages in %.1fs = %.0f msgs/s",
        _processed_count, elapsed, rate,
    )
    _processed_count = 0
    _counter_window_start = now


# --- Windowed table for the rolling average -----------------------------
#
# A Faust "table" is a distributed, partitioned key/value store. Wrapping
# it with `.tumbling(...)` gives us automatic time-windowed aggregation:
# Faust buckets updates into 5-minute windows keyed by wall-clock time and
# handles expiry of old windows for us.

WINDOW_SIZE = 300.0  # 5 minutes, in seconds (spec: "5-minute rolling average")

rolling_temp_table = (
    app.Table(
        "rolling-temp-by-truck",
        default=list,   # each window bucket holds a list of readings
        partitions=6,
    )
    .tumbling(WINDOW_SIZE, expires=WINDOW_SIZE * 2)
    .relative_to_stream()  # window boundaries follow event time from the stream
)


# --- Topology: Consume -> Filter -> Map ---------------------------------

@app.agent(truck_telemetry_topic)
async def process_truck_readings(readings):
    """
    This is the core topology. `readings` is an async iterator Faust feeds
    us as messages arrive on our assigned partitions.
    """
    async for reading in readings:
        global _processed_count
        _processed_count += 1

        # --- Filter: drop clearly-bad sensor readings ---
        # Spec: "Consume -> Filter (Temp > 0) -> Map"
        if reading.temperature_c <= 0:
            continue

        # --- Map: normalize the reading before storing ---
        # Rounding here keeps the windowed aggregation numbers clean; a
        # "map" step is also where you'd do unit conversion, enrichment,
        # or reshaping in a real pipeline.
        mapped_temp = round(reading.temperature_c, 1)

        # --- Windowed aggregation ---
        # Append this reading into the current 5-minute bucket for this
        # truck. `.value()` gets the mutable value for the *current* time
        # window; Faust handles bucket rollover automatically.
        current_bucket = rolling_temp_table[reading.truck_id].value()
        current_bucket.append(mapped_temp)
        rolling_temp_table[reading.truck_id] = current_bucket

        rolling_avg = sum(current_bucket) / len(current_bucket)

        app.logger.debug(
            "truck=%s latest=%.1f°C rolling_avg(5min)=%.2f°C samples=%d",
            reading.truck_id, mapped_temp, rolling_avg, len(current_bucket),
        )


# --- Debug/inspection endpoint ------------------------------------------
#
# Faust apps can also expose a small web server. This is a convenience
# route for manually checking a truck's current rolling average without
# building the full FastAPI/React Flow dashboard yet (that's later in
# Week 3-4 per the spec).

@app.page("/truck/{truck_id}/")
async def get_truck_average(web, request, truck_id: str):
    bucket = rolling_temp_table[truck_id].value()
    if not bucket:
        return web.json({"truck_id": truck_id, "samples": 0, "rolling_avg": None})
    return web.json({
        "truck_id": truck_id,
        "samples": len(bucket),
        "rolling_avg": round(sum(bucket) / len(bucket), 2),
    })


if __name__ == "__main__":
    app.main()
