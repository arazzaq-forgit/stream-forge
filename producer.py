"""
producer.py — Week 1, Project 2 "Stream Forge"

Simulates a fleet of IoT trucks sending temperature telemetry into Kafka.
Each truck sends one reading roughly every `--interval` seconds. Messages
are keyed by truck_id so that Kafka's default partitioner routes all of a
given truck's readings to the same partition — this matters later (Week 2+)
when we do per-truck windowed aggregation, since a stream processor can
only maintain simple local state if a truck's events always land on the
same worker/partition.

Usage:
    python producer.py                      # 50,000 trucks, 10s interval (spec default)
    python producer.py --num-trucks 500 --interval 1   # faster feedback loop while developing
    python producer.py --num-trucks 50 --interval 1 --once   # send one round then exit
"""

import argparse
import json
import random
import time
import signal
import sys
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException


def build_producer(bootstrap_servers: str) -> Producer:
    """
    Configure the producer for high throughput rather than lowest latency:
    - linger.ms batches messages briefly before sending, letting us pack
      more records per request (huge win at 50k+ trucks).
    - compression.type shrinks the small, repetitive JSON payloads.
    - acks=1 is a reasonable default for telemetry (leader ack only); bump
      to 'all' later once we care about zero data loss guarantees.
    """
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 20,
        "batch.size": 64 * 1024,
        "compression.type": "gzip",  # stdlib-only codec, avoids native lz4 dependency issues
        "acks": "all",  # required when enable.idempotence is True
        "retries": 5,
        "enable.idempotence": True,  # avoids duplicate sends on retry
    }
    return Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY FAILED] {msg.key()}: {err}", file=sys.stderr)


def generate_reading(truck_id: int) -> dict:
    """
    Produces one telemetry event. Temperature drifts slowly per truck rather
    than being pure random noise, so downstream rolling-average logic has
    something meaningful to smooth out.
    """
    base_temp = 20 + (truck_id % 15)  # trucks cluster around different baselines
    noise = random.uniform(-2.5, 2.5)
    # occasional spike to simulate a real fault worth catching in aggregation
    spike = 15 if random.random() < 0.002 else 0

    return {
        "truck_id": f"truck-{truck_id:05d}",
        "temperature_c": round(base_temp + noise + spike, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(bootstrap_servers: str, topic: str, num_trucks: int, interval: float, once: bool):
    producer = build_producer(bootstrap_servers)
    stop = {"flag": False}

    def handle_sigint(signum, frame):
        print("\nShutting down, flushing pending messages...")
        stop["flag"] = True

    signal.signal(signal.SIGINT, handle_sigint)

    round_num = 0
    try:
        while not stop["flag"]:
            round_start = time.time()
            for truck_id in range(num_trucks):
                event = generate_reading(truck_id)
                try:
                    producer.produce(
                        topic,
                        key=event["truck_id"],
                        value=json.dumps(event),
                        callback=delivery_report,
                    )
                except BufferError:
                    # local queue full — give librdkafka a moment to drain
                    producer.poll(0.1)
                    producer.produce(
                        topic,
                        key=event["truck_id"],
                        value=json.dumps(event),
                        callback=delivery_report,
                    )

                # poll(0) services delivery callbacks without blocking
                if truck_id % 500 == 0:
                    producer.poll(0)

            producer.flush(10)
            round_num += 1
            elapsed = time.time() - round_start
            print(
                f"[round {round_num}] sent {num_trucks} readings to '{topic}' "
                f"in {elapsed:.2f}s ({num_trucks / max(elapsed, 1e-6):.0f} msgs/s)"
            )

            if once:
                break

            sleep_for = max(0.0, interval - elapsed)
            time.sleep(sleep_for)

    except KafkaException as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        producer.flush(10)
        print("Producer flushed and stopped cleanly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream Forge mock IoT truck telemetry producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="truck-telemetry")
    parser.add_argument("--num-trucks", type=int, default=50_000, help="Fleet size (spec default: 50,000)")
    parser.add_argument("--interval", type=float, default=10.0, help="Seconds between rounds (spec default: 10)")
    parser.add_argument("--once", action="store_true", help="Send a single round then exit (useful for quick tests)")
    args = parser.parse_args()

    run(args.bootstrap_servers, args.topic, args.num_trucks, args.interval, args.once)