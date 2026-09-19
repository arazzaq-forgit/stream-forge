# Stream Forge — Week 1: Kafka Foundation

Goal for this week: spin up a local Kafka cluster and get a high-throughput
Python producer blasting mock IoT truck telemetry into a topic.

### 1. Start Kafka locally

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

### 2. Create the topic

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

---

# Week 2: Stream Topology & Windowing

`stream_app.py` implements the topology using **Faust** (specifically the
`faust-streaming` fork — the original `faust` package on PyPI is
unmaintained; don't install that one).

## 1. Install the new dependency

```bash
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

## 2. Make sure Kafka is running and the producer is sending data

```bash
docker compose up -d
python producer.py --num-trucks 500 --interval 1
```

## 3. Run the Faust worker

In a new terminal (with the venv active):

```bash
faust -A stream_app worker -l info
```

You should see log lines like:

```
truck=truck-00042 latest=21.3°C rolling_avg(5min)=20.87°C samples=14
```

Faust will also spin up a small web server (default `localhost:6066`).
You can hit the debug endpoint to check any truck's current rolling
average directly:

```bash
curl http://localhost:6066/truck/truck-00042/
```

## 4. Simulate multiple parallel workers

Open a second terminal and run the exact same command:

```bash
faust -A stream_app worker -l info
```

Faust's consumer group protocol will automatically split the topic's 6
partitions across both workers — watch the logs to see each worker only
processing a subset of truck IDs. This is the foundation for Week 3's
chaos test (killing a worker and watching partitions rebalance).

## Design notes

- **Tumbling window, 5 minutes**: matches the spec's "5-minute rolling
  average temperature per truck." Faust buckets events into fixed,
  non-overlapping time windows and expires old ones automatically.
- **`relative_to_stream()`**: window boundaries follow event time carried
  in the stream rather than wall-clock processing time — this is what
  lets the system "handle late-arriving data gracefully," one of the
  Week 2 spec's explicit checks.
- **RocksDB-backed table (`store="rocksdb://"`)**: this is the same
  mechanism Week 3 builds on for "Stateful Recovery" — the rolling
  averages are checkpointed to disk (and, once configured, to a Kafka
  changelog topic) so a crashed worker's state isn't lost.
- **Filter before Map**: readings with `temperature_c <= 0` are dropped
  before any transformation, matching the spec's stated topology order.

## Next up (Week 3)

Wire the table's changelog to Kafka explicitly, add the mid-project
throughput audit (100k events/sec), and run the chaos test: kill a worker
mid-calculation and confirm the rolling average survives via partition
rebalancing + RocksDB changelog recovery.
