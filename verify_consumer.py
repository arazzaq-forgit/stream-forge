"""
verify_consumer.py — quick sanity check, not part of the Week 1 spec deliverables.

Reads and prints a handful of messages from the topic so you can confirm the
producer is actually working before building the Faust/Bytewax topology in Week 2.

Usage:
    python verify_consumer.py --max-messages 20
""""

import argparse
import json

from confluent_kafka import Consumer


def run(bootstrap_servers: str, topic: str, max_messages: int):
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "verify-consumer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])

    seen = 0
    print(f"Listening on '{topic}' (Ctrl+C to stop early)...")
    try:
        while seen < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            print(f"partition={msg.partition()} key={msg.key().decode()} value={event}")
            seen += 1
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"\nDone — saw {seen} messages.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="truck-telemetry")
    parser.add_argument("--max-messages", type=int, default=20)
    args = parser.parse_args()

    run(args.bootstrap_servers, args.topic, args.max_messages)
