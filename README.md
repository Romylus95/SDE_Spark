# SDE_Spark

**Synopsis Data Engine** — A distributed system for maintaining and querying probabilistic data structures (synopses) over continuous Kafka streams, built on Apache Spark Structured Streaming.

This branch (`cluster/softnet-hdp3-hybrid`) targets the **SoftNet HDP 3.1.0 cluster** (Java 8, Spark 2.3.2, Scala 2.11, Kafka 2.0.0) and implements the **hybrid architecture** that reduces HDFS checkpoint I/O from 3 stateful operators to 1.

---

## Hybrid Architecture

The key architectural improvement over the baseline is reducing stateful operators from 3 to 1, directly cutting HDFS checkpoint writes per micro-batch by two thirds.

| Layer | Component | Type | Description |
|-------|-----------|------|-------------|
| 1 | `KafkaIngestionLayer` | stateless | Consume JSON from Kafka, deserialize to typed POJOs |
| 2 | `StatelessRouter` | **stateless** | Hash-route data to KEYED slots, fan-out requests — no state |
| 3 | `SynopsisProcessor` | **stateful** | Synopsis lifecycle: ADD / ESTIMATE / DELETE / timeout |
| 4 | `PathSplitter` | stateless | GREEN (noOfP=1) or PURPLE (noOfP>1) path split |
| 5 | `ReduceAggregator` | **stateless** | Merge N partial estimates within the same micro-batch |
| 6 | `KafkaOutputLayer` | stateless | Serialize estimations to JSON, write to Kafka |

### Why StatelessRouter has no state

Routing decisions are fully determined by `--num-slots N` set at spark-submit time — a fixed job-level parameter. Data events are assigned to slot `streamID.hashCode() % N`; requests are fanned out to all N slots. No registration map needs to survive across batches.

### Why ReduceAggregator has no state

All N partial ESTIMATE results for a given `uid` arrive in the same micro-batch: the ESTIMATE request is fanned out to all N slots simultaneously, all N SynopsisProcessor instances emit their partial in the same batch, and `groupByKey(uid).flatMapGroups()` merges them within that batch. No cross-batch buffering is needed.

### Why SynopsisProcessor must remain stateful

The synopsis data structures (CountMin, BloomFilter, AMS, HyperLogLog) accumulate data incrementally across the entire lifetime of the stream. They cannot be recomputed per batch — the state IS the computation.

---

## Pipeline Flow

```
Kafka (data_topic + request_topic)
        │
        ▼
Layer 1  KafkaIngestionLayer     JSON bytes → Datapoint / Request POJOs
        │
        ▼
Layer 2  StatelessRouter         data → one KEYED slot  |  request → all N slots
        │
        ▼
Layer 3  SynopsisProcessor       ADD / ESTIMATE / DELETE / TTL eviction   [stateful]
        │
        ├── noOfP=1 (GREEN) ──────────────────────────────────────────────┐
        │                                                                  │
        └── noOfP>1 (PURPLE) ──►  Layer 4  PathSplitter                   │
                                           │                               │
                                           ▼                               │
                                  Layer 5  ReduceAggregator                │
                                  (merge N partials, same batch)           │
                                           │                               │
                                           └────────────────────┬──────────┘
                                                                │
                                                                ▼
                                                       Layer 6  KafkaOutputLayer
                                                       estimation_topic
```

Micro-batch trigger: `ProcessingTime("1 second")`. State checkpointed to HDFS every batch.

---

## Synopsis Algorithms

| ID | Class | Purpose | Result type |
|----|-------|---------|-------------|
| 1 | `CountMin` | Frequency / sum estimation | Long |
| 2 | `Bloomfilter` | Membership testing | Boolean |
| 3 | `AMSsynopsis` | Frequency moment F2 | Double |
| 4 | `HyperLogLogSynopsis` | Cardinality (distinct count) | Long |

---

## Technology Stack (this branch)

| Component | Version |
|-----------|---------|
| Apache Spark | 2.3.2 |
| Scala | 2.11.12 |
| Java | 8 |
| Apache Kafka | 2.0.0 |
| Hadoop / HDFS | HDP 3.1.0 |
| Kryo | (via Spark) |
| Jackson | 2.15.3 |
| stream-lib (Clearspring) | 2.9.5 |
| streaminer | 1.1.1 |

---

## HDP 3.1.0 Cluster Patch

`KafkaOffsetReader` is patched to fix a coordinator hang specific to this cluster. Approximately 21/50 `__consumer_offsets` partitions have `Leader: -1` (dead broker replicas), causing `FindCoordinator` to return `COORDINATOR_NOT_AVAILABLE` and the Spark driver to hang indefinitely.

The bundled `src/main/scala/org/apache/spark/sql/kafka010/KafkaOffsetReader.scala` replaces `consumer.poll(0)` + seek + `position()` with `beginningOffsets()` / `endOffsets()`, which send `ListOffsets` directly to partition leaders with no coordinator contact. The shade plugin excludes the original class from the bundled kafka jar so the patched version takes precedence.

---

## Build

```bash
mvn clean package -DskipTests
```

Output JAR: `target/sde-spark-1.0.0-SNAPSHOT.jar`

---

## Cluster Deployment (SoftNet HDP 3.1.0)

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors N \
  --executor-cores 2 \
  --executor-memory 4g \
  --conf spark.sql.shuffle.partitions=N \
  --conf spark.hadoop.dfs.replication=1 \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatInterval=60s \
  --class infore.sde.spark.SDESparkApp \
  sde-spark-1.0.0-SNAPSHOT.jar \
  --kafka-brokers <broker>:6667 \
  --checkpoint-location hdfs:///sde/checkpoints \
  --num-slots N \
  --kafka-partitions N \
  --max-offsets-per-trigger $((N * 50000)) \
  --ingestion-multiplier 10 \
  --trigger-interval "1 second"
```

Set `--num-slots N`, `--kafka-partitions N`, `--num-executors N`, and `spark.sql.shuffle.partitions=N` to the same value to ensure one Kafka partition per executor and a single shuffle wave per batch.

---

## Key Configuration Parameters

| Flag | Default | Description |
|------|---------|-------------|
| `--num-slots` | `1` | Number of parallel synopsis worker slots (PURPLE path) |
| `--kafka-partitions` | `4` | Kafka partitions to read from; must match `--num-slots` |
| `--max-offsets-per-trigger` | unlimited | Total Kafka messages per micro-batch; set to `N × 50000` |
| `--ingestion-multiplier` | `1` | In-pipeline tuple duplication factor for benchmarking |
| `--kafka-brokers` | `localhost:9092` | Kafka bootstrap servers |
| `--checkpoint-location` | `hdfs:///sde/checkpoints` | HDFS path for Spark state checkpoints |
| `--trigger-interval` | `1 second` | Micro-batch trigger interval |
| `--data-topic` | `data_topic` | Kafka topic for incoming data events |
| `--request-topic` | `request_topic` | Kafka topic for synopsis management requests |
| `--output-topic` | `estimation_topic` | Kafka topic for estimation results |

---

## Kafka Message Contract

**Data event** (`data_topic`):
```json
{
  "dataSetkey": "Forex",
  "streamID":   "EURUSD",
  "values": { "StockID": "AAPL", "price": "182.50" }
}
```

**Request** (`request_topic`) — `requestID % 10`: `1`=ADD, `2`=DELETE, `3`=ESTIMATE:
```json
{ "dataSetkey": "Forex", "requestID": 1, "synopsisID": 1, "uid": 42,
  "param": ["StockID","price","Queryable","0.01","0.99","42"], "noOfP": 4 }
```

**Estimation** (`estimation_topic`):
```json
{ "key": "Forex", "uid": 42, "requestID": 3, "synopsisID": 1,
  "estimation": "549.0", "param": ["AAPL"], "noOfP": 4 }
```

---

## Load Generation (KafkaPreloader)

```bash
java -cp sde-spark-1.0.0-SNAPSHOT.jar \
  infore.sde.spark.integration.KafkaPreloader \
  --brokers <broker>:6667 \
  --base-messages 100000 \
  --multiplier 100 \
  --noop 4 \
  --synopsis-types 1
```

`--base-messages × --multiplier` = total Kafka messages produced (e.g. 100K × 100 = 10M).
The pipeline's `--ingestion-multiplier` further duplicates each message in-pipeline (e.g. ×10 → 100M effective tuples processed).

---

## Experiment Results (SoftNet Cluster)

All experiments: noOfP=4 baseline, CountMin (ID=1), 4 dataset keys, `ingestion-multiplier=10`, PURPLE path, `dfs.replication=1`.

### Experiment A — Throughput vs Parallelization Degree (Option B Scaled)

Scaled `maxOffsetsPerTrigger = N × 50,000` and Kafka partitions = N per run for a fair per-executor workload comparison.

| noOfP | Throughput (rows/s) | Elapsed (s) | vs N=2 | Scaling eff. |
|-------|--------------------:|------------:|--------|-------------|
| 2  | 70,763  | 1,418 | baseline | baseline |
| 4  | 134,275 | 750   | +90%    | 95% |
| 6  | 183,747 | 549   | +160%   | 87% |
| 8  | 238,886 | 428   | +238%   | 85% |
| 10 | 288,729 | 356   | +308%   | 82% |

Near-linear scaling: **4.1× throughput with 5× workers** (81% average efficiency).

### Experiment B — Throughput vs Message Volume

| Volume | Throughput (rows/s) | Elapsed (s) |
|--------|--------------------:|------------:|
| 5M     | 116,753 | 437   |
| 10M    | 126,022 | 801   |
| 20M    | 120,029 | 1,676 |

Throughput is **volume-independent** (spread < 8%). Elapsed scales linearly.

### Experiment C — Throughput vs Stream Count

| Streams | Throughput (rows/s) | Elapsed (s) |
|---------|--------------------:|------------:|
| 50      | 126,599 | 802 |
| 500     | 127,314 | 792 |
| 5,000   | 119,606 | 842 |

**O(1) routing confirmed**: throughput insensitive to stream count across a 100× range.

### Experiment D — Throughput vs Synopsis Algorithm

| Synopsis | Throughput (rows/s) | Elapsed (s) |
|----------|--------------------:|------------:|
| CountMin    | 124,828 | 813 |
| BloomFilter | 124,364 | 811 |
| AMS         | 139,442 | 723 |
| HyperLogLog | 133,251 | 757 |

All four algorithms within 12% of each other — pipeline infrastructure dominates, not synopsis computation.

### Experiment E — Accuracy Validation

| Synopsis | Query | Result | Verdict |
|----------|-------|--------|---------|
| CountMin (ε=0.002) | AAPL price sum | 6,069 | PASS |
| BloomFilter (FPR=0.01) | AAPL membership | true | PASS |
| AMS (b=100, d=5) | AAPL freq. est. | 7,157 | PASS |
| HyperLogLog (sd=5%) | distinct prices | 1,077 | PASS |

All four synopsis algorithms produce correct estimates within their theoretical error guarantees.

---

## Project Structure

```
src/main/java/infore/sde/spark/
├── SDESparkApp.java                    Entry point — wires all 6 layers
├── config/SDEConfig.java               CLI argument parsing + defaults
├── ingestion/KafkaIngestionLayer.java  Layer 1 — Kafka consumer + JSON parsing
├── routing/
│   ├── StatelessRouter.java            Layer 2 — stateless fan-out (no HDFS state)
│   ├── DataRouter.java                 (baseline reference, not used in hybrid)
│   └── RequestRouter.java              Fan-out helpers
├── processing/
│   ├── SynopsisProcessor.java          Layer 3 — stateful synopsis lifecycle
│   ├── SynopsisProcessorState.java     State: Map<uid, Synopsis>
│   └── InputEvent.java                 Union discriminator (DATA | REQUEST)
├── aggregation/
│   ├── PathSplitter.java               Layer 4 — GREEN / PURPLE split
│   └── ReduceAggregator.java           Layer 5 — stateless in-batch merge
├── output/KafkaOutputLayer.java        Layer 6 — Kafka producer
├── messages/                           Datapoint, Request, Estimation POJOs
├── synopses/                           Synopsis algorithms + factory
├── reduceFunctions/                    SimpleSumFunction, SimpleORFunction
├── metrics/                            PipelineMetrics, ThroughputListener
└── integration/                        KafkaPreloader, test apps
src/main/scala/org/apache/spark/sql/kafka010/
└── KafkaOffsetReader.scala             HDP 3.1.0 coordinator hang patch
```

---

## Tests

```bash
mvn test
```

12 unit test classes covering routing, synopsis lifecycle, serialization, aggregation functions, and configuration parsing.
