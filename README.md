# Stream Forge — Week 1: Kafka Foundation

Goal for this week: spin up a local Kafka cluster and get a high-throughput
Python producer blasting mock IoT truck telemetry into a topic.

## 1. Start Kafka locally

Requires Docker + Docker Compose.

```bash
docker compose up -d
```

This starts:
- **Kafka** (KRaft mode, no Zookeeper) on `localhost:9092`
- **Kafka UI** at [http://localhost:8080](http://localhost:8080) — use this to watch
  topics, partitions, and message rates visually as you develop

Give it ~15 seconds to finish starting, then check:

```bash
docker compose ps
```

## 2. Create the topic

```bash
docker exec streamforge-kafka kafka-topics --create \
  --topic truck-telemetry \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1
```

(6 partitions matches the spec's later "20 parallel workers" scenario —
you'll see how partition count caps parallelism when you get to Week 4.)

## 3. Install Python dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 4. Run the producer

Start small while developing — the full 50,000-truck fleet is easy to enable
once you trust the pipeline:

```bash
python producer.py --num-trucks 500 --interval 1
```

Once you're confident it works, run it at spec scale:

```bash
python producer.py --num-trucks 50000 --interval 10
```

You should see throughput logs like:

```
[round 1] sent 500 readings to 'truck-telemetry' in 0.34s (1470 msgs/s)
```

## 5. Verify messages are landing

In a second terminal:

```bash
python verify_consumer.py --max-messages 20
```

You should see individual truck readings with their partition assignment,
e.g. `partition=3 key=truck-00042 value={...}`.

Or just watch it visually in Kafka UI at `localhost:8080` → Topics →
`truck-telemetry`.

## Design notes

- **Keyed by `truck_id`**: every reading from a given truck always lands on
  the same partition. This is what makes per-truck windowed aggregation
  (Week 2's rolling average) tractable — a stream processor can maintain
  simple local state per partition instead of needing a distributed join.
- **Idempotent producer + acks=1**: reasonable middle ground for telemetry.
  You can experiment with `acks=all` later to see the throughput trade-off,
  which ties nicely into the Week 2 "exactly-once" discussion.
- **Batching (`linger.ms`, `compression.type`)**: this is what lets a single
  Python process realistically approach the "100,000 events/sec" mid-project
  throughput target — sending one-message-per-request would bottleneck far
  earlier.

## Next up (Week 2)

Build the Faust/Bytewax stream topology: `Consume → Filter (Temp > 0) → Map`,
then implement the 5-minute tumbling window for the rolling average.
