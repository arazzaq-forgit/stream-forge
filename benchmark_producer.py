"""
benchmark_producer.py — Week 3, Project 2 "Stream Forge"

The spec's mid-project throughput audit asks: "Prove the Python workers
can consume and process 100,000 events per second across multiple local
partitions."

This script is the PRODUCER side of that proof: it sends readings as fast
as physically possible (no sleep between rounds) for a fixed duration and
reports the actual achieved throughput. Pair it with watching the Faust
worker's consumer lag (see the README) to see the CONSUMER side — whether
processing keeps up with ingestion, or falls behind.

Honest framing for your portfolio writeup: hitting a literal 100,000
msgs/sec on a single laptop with 6 partitions is unlikely — that number in
the original spec assumes a multi-broker cluster and multiple physical
machines running "20 parallel worker nodes." What THIS script proves is
something more valuable to actually understand: where YOUR machine's
bottleneck sits (network loopback, JSON serialization, partition count,
single-broker fsync), and by how much. That's a more defensible, more
interesting result than a inflated number, and it's exactly the kind of
thing a "Performance Audit" review is meant to surface.

Usage:
    python benchmark_producer.py --duration 15
    python benchmark_producer.py --duration 30 --num-trucks 5000
"""

import argparse
import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


def build_producer(bootstrap_servers: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 20,
        "batch.size": 256 * 1024,  # larger batches matter more at max throughput
        "compression.type": "gzip",
        "acks": "all",
        "enable.idempotence": True,
        "queue.buffering.max.messages": 500_000,
        "queue.buffering.max.kbytes": 512_000,
    }
    return Producer(conf)


def generate_reading(truck_id: int) -> bytes:
    return json.dumps({
        "truck_id": f"truck-{truck_id:05d}",
        "temperature_c": 20.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }).encode("utf-8")


def run(bootstrap_servers: str, topic: str, num_trucks: int, duration: float):
    producer = build_producer(bootstrap_servers)

    sent = 0
    delivered = 0
    failed = 0

    def delivery_report(err, msg):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
        else:
            delivered += 1

    print(f"Sending as fast as possible for {duration:.0f}s against {num_trucks} truck keys...")
    start = time.time()
    truck_id = 0

    while time.time() - start < duration:
        try:
            producer.produce(
                topic,
                key=f"truck-{truck_id % num_trucks:05d}",
                value=generate_reading(truck_id % num_trucks),
                callback=delivery_report,
            )
            sent += 1
            truck_id += 1
        except BufferError:
            # local queue is full — this itself is useful signal: it means
            # OUR producer can generate messages faster than the network/
            # broker can absorb them. poll() drains delivery callbacks and
            # frees space.
            producer.poll(0.01)

        if sent % 10_000 == 0:
            producer.poll(0)

    send_elapsed = time.time() - start
    print(f"Finished generating {sent} messages in {send_elapsed:.2f}s "
          f"({sent / send_elapsed:,.0f} msgs/s produced, pre-flush)")

    print("Flushing remaining buffer (waiting for broker acks)...")
    flush_start = time.time()
    producer.flush(60)
    total_elapsed = time.time() - flush_start + send_elapsed

    print()
    print("=" * 60)
    print("THROUGHPUT AUDIT RESULTS")
    print("=" * 60)
    print(f"  Messages sent:        {sent:,}")
    print(f"  Delivered (acked):    {delivered:,}")
    print(f"  Failed:               {failed:,}")
    print(f"  Wall-clock time:      {total_elapsed:.2f}s (send + flush)")
    print(f"  Sustained throughput: {delivered / total_elapsed:,.0f} msgs/s")
    print("=" * 60)
    print()
    print("Compare this number against the spec's 100k/s target. If you're")
    print("well below it, the likely bottlenecks (roughly in order of impact")
    print("on a single-machine dev setup) are:")
    print("  1. Single Kafka broker (no parallelism across broker nodes)")
    print("  2. Only 6 partitions (caps producer/consumer parallelism)")
    print("  3. acks=all (waits for full durability per batch)")
    print("  4. JSON serialization overhead vs. a binary format like Avro")
    print("  5. Running inside WSL2 (adds a virtualization network hop)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream Forge throughput benchmark")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="truck-telemetry")
    parser.add_argument("--num-trucks", type=int, default=1000)
    parser.add_argument("--duration", type=float, default=15.0, help="Seconds to send at max rate")
    args = parser.parse_args()

    run(args.bootstrap_servers, args.topic, args.num_trucks, args.duration)