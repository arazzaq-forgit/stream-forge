"""
windowing_check.py — Mid-Project Review, Project 2 "Stream Forge"

Spec's windowing check: "Verify that the 5-minute rolling average
calculations are mathematically correct and handle late-arriving data
gracefully."

This script does two things automatically:
  1. Sends a known sequence of readings for one test truck and computes
     what the rolling average SHOULD be by hand (in this script).
  2. Sends one deliberately "late" reading (an old timestamp) and confirms
     the pipeline doesn't crash or silently drop it.

Then it queries the Faust worker's debug endpoint and compares its
reported rolling average against the expected value.

IMPORTANT: run this against a fresh/quiet topic — if the producer.py or
benchmark_producer.py are also running and sending data for the same
truck_id, the comparison will be thrown off. Use a dedicated test truck ID
that nothing else is writing to.

Usage:
    # Terminal 1: faust -A stream_app worker -l info  (already running)
    python windowing_check.py
"""

import json
import time
from datetime import datetime, timezone, timedelta

import requests
from confluent_kafka import Producer

TEST_TRUCK_ID = f"truck-TEST-{int(__import__('time').time())}"
FAUST_WEB_URL = "http://localhost:6066"


def build_producer():
    return Producer({
        "bootstrap.servers": "localhost:9092",
        "acks": "all",
        "enable.idempotence": True,
    })


def send_reading(producer, truck_id: str, temp: float, timestamp: datetime):
    event = {
        "truck_id": truck_id,
        "temperature_c": temp,
        "timestamp": timestamp.isoformat(),
    }
    producer.produce("truck-telemetry", key=truck_id, value=json.dumps(event))
    producer.flush(5)


def check_faust_endpoint(truck_id: str) -> dict:
    resp = requests.get(f"{FAUST_WEB_URL}/truck/{truck_id}/", timeout=5)
    resp.raise_for_status()
    return resp.json()


def main():
    producer = build_producer()
    now = datetime.now(timezone.utc)

    print(f"Testing with dedicated truck ID: {TEST_TRUCK_ID}")
    print("(make sure no other producer is writing to this ID)\n")

    # --- Part 1: known sequence, verify math ---
    known_temps = [20.0, 22.0, 21.0, 23.0, 19.0]
    expected_avg = sum(known_temps) / len(known_temps)

    print(f"Sending {len(known_temps)} readings: {known_temps}")
    for temp in known_temps:
        send_reading(producer, TEST_TRUCK_ID, temp, datetime.now(timezone.utc))
        time.sleep(0.3)  # give Faust time to process each one in order

    print("Waiting 3s for Faust to catch up...")
    time.sleep(3)

    try:
        result = check_faust_endpoint(TEST_TRUCK_ID)
        actual_avg = result.get("rolling_avg")
        print(f"\nExpected average: {expected_avg:.2f}°C")
        print(f"Faust reported:   {actual_avg}°C")
        print(f"Sample count:     {result.get('samples')} (expected {len(known_temps)})")

        if actual_avg is not None and abs(actual_avg - expected_avg) < 0.5:
            print("✅ PASS: rolling average math is correct")
        else:
            print("❌ MISMATCH: check if other traffic is also writing to this truck ID")
    except Exception as e:
        print(f"❌ Could not reach Faust debug endpoint: {e}")
        print("   Is the Faust worker running? (faust -A stream_app worker -l info)")
        return

    # --- Part 2: late-arriving data ---
    print("\n" + "=" * 60)
    print("Testing late-arriving data...")
    late_timestamp = now - timedelta(minutes=2)  # 2 minutes in the past
    print(f"Sending a reading timestamped {late_timestamp.isoformat()} "
          f"(2 minutes 'late' relative to now)")

    send_reading(producer, TEST_TRUCK_ID, 99.9, late_timestamp)
    time.sleep(3)

    try:
        result_after_late = check_faust_endpoint(TEST_TRUCK_ID)
        print(f"\nAfter late reading — samples: {result_after_late.get('samples')}, "
              f"rolling_avg: {result_after_late.get('rolling_avg')}")
        if result_after_late.get("samples", 0) > result.get("samples", 0):
            print("✅ PASS: late-arriving reading was accepted and incorporated, "
                  "not dropped or crashed on")
        else:
            print("⚠️  Sample count didn't increase — check worker logs for errors")
    except Exception as e:
        print(f"❌ Error checking after late reading: {e}")


if __name__ == "__main__":
    main()