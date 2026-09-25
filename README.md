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

---

# Week 3: Throughput Audit & Chaos Testing

Two things this week, matching the spec's Mid-Project Review checklist:
1. **Performance Audit** — measure actual throughput against the 100k/s target
2. **Chaos Testing** — kill a worker and prove state survives

## Part 1: Throughput Audit

Make sure Kafka is running (`docker compose up -d`) and no other producer
is currently flooding the topic (stop `producer.py` if it's running).

```bash
python benchmark_producer.py --duration 15
```

This sends messages as fast as the producer physically can (no artificial
delay) for 15 seconds, then reports sustained throughput.

**Be honest about the number you get.** A single laptop with one Kafka
broker and 6 partitions realistically will NOT hit 100,000 msgs/sec — the
original spec's number assumes a proper multi-broker cluster and multiple
physical worker machines. What matters for the audit is:

- What throughput did you actually achieve?
- What's the bottleneck? (The script prints likely culprits at the end.)
- What would you change to get closer to spec, if you had real cluster
  hardware? (More partitions, more brokers, binary serialization instead
  of JSON, `acks=1` instead of `acks=all`, etc.)

This kind of grounded, bottleneck-aware analysis is worth more in an
interview than a suspiciously round "yep, hit 100k" claim.

## Part 1b: Consumer-side throughput (the other half of the audit)

`stream_app.py` now includes a built-in throughput counter that reports
every 5 seconds — this measures how fast the Faust worker itself can
actually pull messages off Kafka and run them through filter → map →
windowed aggregation, which is a separate number from how fast the
producer can write.

```bash
# Terminal 1: Kafka already running
# Terminal 2:
python benchmark_producer.py --duration 30

# Terminal 3 (start this just before or during the benchmark):
faust -A stream_app worker -l info
```

Watch for lines like:

```
CONSUMER THROUGHPUT: 48213 messages in 5.0s = 9642 msgs/s
```

**Compare this against your producer-side number.** If producer throughput
was, say, 136,000 msgs/s but consumer throughput is only ~10,000 msgs/s,
that gap is the real story: Kafka can absorb writes far faster than a
single Python worker can deserialize JSON and update windowed state for
each one. This is exactly why the spec calls for "20 parallel Python
worker nodes" — one worker alone isn't meant to keep up at full scale.
Try running 2-3 Faust workers at once (see Part 2 below for how) and
watch the aggregate consumer throughput across all of them climb as work
spreads across partitions.

## Part 2: Chaos Testing (partition rebalancing + state recovery)

This proves the spec's core distributed-systems claim: *"If Worker Node #4
crashes, StreamForge automatically rebalances the partition to Worker #5
and recovers its state from a RocksDB changelog, ensuring no sensor
reading is ever dropped or processed twice."*

**Setup — you'll need 4 terminals open:**

1. Producer: `python producer.py --num-trucks 500 --interval 1`
2. Faust worker A: `faust -A stream_app worker -l info --web-port 6066`
3. Faust worker B: `faust -A stream_app worker -l info --web-port 6067`
   (different `--web-port` since both workers can't share one)
4. Free terminal for running commands / watching Kafka UI

**Steps:**

1. Start the producer, then both Faust workers. Watch the logs — Faust's
   consumer group will split the 6 partitions across the two workers
   (e.g. worker A gets partitions 0-2, worker B gets 3-5). You'll see this
   in the "Setting newly assigned partitions" log line on each worker.

2. Let it run for a couple minutes so both workers accumulate real rolling
   averages for different trucks (check the logs for `rolling_avg` lines
   from each worker — they should be processing *different* truck IDs,
   proof the partitioning is working).

3. **Kill worker B** — go to its terminal and press `Ctrl+C`, or for a
   harsher (more realistic "crash") test, find its process ID and
   `kill -9 <pid>` instead of a graceful shutdown.

4. **Watch worker A's terminal.** Within a few seconds you should see log
   lines about "Rebalancing", "Revoking previously assigned partitions",
   and then worker A picking up worker B's old partitions
   ("Setting newly assigned partitions" showing all 6 now).

5. **Check recovery**: once worker A takes over the extra partitions, look
   for `[^---Recovery]: Restore complete!` in its logs — this is Faust
   replaying the RocksDB changelog topic to rebuild state for the trucks
   it just inherited, rather than starting their rolling averages from
   zero.

6. **Verify with the debug endpoint** — hit a truck ID that was previously
   owned by the killed worker:
   ```bash
   curl http://localhost:6066/truck/truck-00042/
   ```
   If the `samples` count and `rolling_avg` look continuous (not reset to
   1 sample), that's your proof: state survived the crash.

## Design notes

- **Why `acks=all` matters here**: it's what makes the idempotent producer
  safe against duplicate sends during a broker hiccup — directly relevant
  to the spec's "no sensor reading is ever... processed twice" claim.
- **RocksDB changelog topic**: Faust automatically created
  `stream-forge-rolling-temp-by-truck-changelog` — every table update is
  also written there. This is what a surviving worker replays to rebuild
  a crashed worker's state, instead of that data being gone forever.
- **Graceful vs. hard kill**: `Ctrl+C` lets Faust leave the consumer group
  cleanly (faster rebalance). `kill -9` simulates a real crash — Kafka has
  to wait for a session timeout before reassigning partitions, which is a
  more realistic (and slower) test of the recovery path.

## Next up (Week 4)

Multi-stream orchestration with asyncio, Prometheus metrics export, and
polishing the telemetry dashboard.