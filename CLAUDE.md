# CLAUDE.md — SDE_Spark Project Reference

## Project Overview

**SDE_Spark** is a standalone Apache Spark 3.5.3 implementation of the Synopsis Data Engine (SDE) — a distributed system that maintains probabilistic data structures (synopses) over continuous Kafka streams. It processes real-time events, manages in-memory synopsis algorithm instances, and answers estimation queries with sub-linear space and time complexity.

- **Language:** Java 17
- **Build:** Maven 3.x with Shade plugin (fat JAR)
- **Entry point:** `src/main/java/infore/sde/spark/SDESparkApp.java`
- **Output JAR:** `target/sde-spark-1.0.0-SNAPSHOT.jar`

---

## 6-Layer Pipeline Architecture

```
Kafka (data_topic + request_topic)
        ↓
Layer 1  KafkaIngestionLayer     JSON bytes → Datapoint/Request POJOs (Jackson + Kryo)
        ↓
Layer 2  DataRouter              Union stream, hash-route data, fan-out requests
        ↓
Layer 3  SynopsisProcessor       Synopsis lifecycle: ADD / DATA / ESTIMATE / DELETE / TIMEOUT
        ↓
Layer 4  PathSplitter            noOfP==1 → GREEN (direct)  |  noOfP>1 → PURPLE (aggregation)
        ↓
Layer 5  ReduceAggregator        Collect N partial results, merge via reduce function
        ↓
Layer 6  KafkaOutputLayer        Estimation → JSON → estimation_topic
```

Micro-batch trigger: `ProcessingTime("1 second")`. State checkpointed to HDFS/S3 every batch.

---

## Source File Map

```
src/main/java/infore/sde/spark/
├── SDESparkApp.java                    Entry point — wires all 6 layers
├── config/SDEConfig.java               All config fields + CLI arg parsing
├── ingestion/KafkaIngestionLayer.java  Layer 1
├── routing/
│   ├── DataRouter.java                 Layer 2 — stateful routing operator
│   ├── RoutingState.java               State: registrations map
│   └── RequestRouter.java              Helpers for request fan-out
├── processing/
│   ├── SynopsisProcessor.java          Layer 3 — stateful synopsis lifecycle
│   ├── SynopsisProcessorState.java     State: Map<uid, Synopsis>
│   └── InputEvent.java                 Union discriminator (DATA | REQUEST)
├── aggregation/
│   ├── PathSplitter.java               Layer 4 — filter GREEN / PURPLE
│   ├── ReduceAggregator.java           Layer 5 — merge partial results
│   └── AggregationState.java           State: partial buffer + reducer
├── output/KafkaOutputLayer.java        Layer 6
├── messages/
│   ├── Datapoint.java                  Data event POJO
│   ├── Request.java                    Synopsis management command POJO
│   └── Estimation.java                 Query result POJO
├── synopses/
│   ├── Synopsis.java                   Abstract base (add / estimate / merge / snapshotState)
│   ├── SynopsisFactory.java            Factory: create(synopsisID, uid, params)
│   ├── CountMin.java                   ID=1  frequency estimation
│   ├── Bloomfilter.java                ID=2  membership testing
│   ├── AMSsynopsis.java                ID=3  frequency moment F2
│   ├── HyperLogLogSynopsis.java        ID=4  cardinality estimation
│   └── sketches/CM.java                Internal CountMin sketch
├── reduceFunctions/
│   ├── ReduceFunction.java             Abstract: add(partial) + reduce()
│   ├── SimpleSumFunction.java          Sum reducer — CountMin, AMS, HyperLogLog
│   └── SimpleORFunction.java           Boolean OR reducer — BloomFilter
├── metrics/
│   ├── PipelineMetrics.java            Spark accumulators
│   └── ThroughputListener.java         Per-batch CSV metrics writer
└── integration/                        Test apps and load producers
    ├── EndToEndTestApp.java
    ├── LoadTestProducer.java
    └── ... (5 more)
```

---

## Kafka Topics & Message Schemas

### data_topic → Datapoint.java
```json
{
  "dataSetkey": "Forex",
  "streamID":   "EURUSD",
  "values": {
    "StockID": "AAPL",
    "price":   "182.50"
  }
}
```
- `dataSetkey` — partition/routing key (used as `groupByKey` key)
- `streamID` — determines which worker slot receives the data (PURPLE path hash)
- `values` — arbitrary JSON payload; algorithm accesses fields by name via params

### request_topic → Request.java

All three operations share the same message shape. `requestID % 10` determines the operation.

- `requestID`: `1`=ADD, `2`=DELETE, `3`=ESTIMATE  (actual op = `requestID % 10`)
- `synopsisID`: `1`=CountMin, `2`=BloomFilter, `3`=AMS, `4`=HyperLogLog
- `uid`: unique synopsis instance ID
- `noOfP`: `1`=GREEN (single worker), `>1`=PURPLE (parallel fan-out)

#### ADD — all fields required except `streamID`
```json
{
  "dataSetkey": "Forex",
  "requestID":  1,
  "synopsisID": 1,
  "uid":        42,
  "param":      ["StockID", "price", "Queryable", "0.01", "0.99", "42"],
  "noOfP":      1
}
```
`synopsisID`, `uid`, and `param` are passed directly to `SynopsisFactory.create()`. `param` contents depend on algorithm (see Synopsis Algorithms section). `streamID` is not used for ADD and is stripped by DataRouter before forwarding.

#### DELETE — only `dataSetkey`, `requestID`, `uid` are needed
```json
{
  "dataSetkey": "Forex",
  "requestID":  2,
  "uid":        42
}
```
All other fields (`synopsisID`, `streamID`, `param`, `noOfP`) are stripped by DataRouter. Fan-out parallelism is derived from the stored registration, same as ESTIMATE.

#### ESTIMATE — only `dataSetkey`, `requestID`, `uid`, `param` needed
```json
{
  "dataSetkey": "Forex",
  "requestID":  3,
  "uid":        42,
  "param":      ["AAPL"]
}
```
`uid` identifies which synopsis to query. `param[0]` is the lookup key for CountMin, BloomFilter, and AMS. HyperLogLog ignores `param` entirely (returns cardinality count). `synopsisID`, `streamID`, and `noOfP` are all stripped by DataRouter — `noOfP` is derived from the stored registration; `synopsisID` is restored by `SynopsisProcessor` from the stored synopsis object.

#### Required fields per operation

| Field | ADD | DELETE | ESTIMATE |
|-------|-----|--------|----------|
| `dataSetkey` | required | required | required |
| `requestID` | `1` | `2` | `3` |
| `uid` | required | required | required |
| `synopsisID` | required | **not used** (stripped) | **not used** (stripped, restored from stored synopsis) |
| `noOfP` | required | **not used** (stripped, read from registration) | **not used** (stripped, read from registration) |
| `streamID` | **not used** (stripped) | **not used** (stripped) | **not used** (stripped) |
| `param` | required (algorithm config) | **not used** (stripped) | `param[0]` = query key (except HLL) |

### estimation_topic → Estimation.java

#### Normal ESTIMATE result
```json
{
  "key":           "Forex",
  "estimationkey": "42",
  "streamID":      "ALL",
  "uid":           42,
  "requestID":     3,
  "synopsisID":    1,
  "estimation":    "549.0",
  "param":         ["AAPL"],
  "noOfP":         1
}
```
- `estimation` type: `Long` (CountMin/HLL), `Double` (AMS), `Boolean` (BloomFilter)

#### TTL eviction notice (`requestID = -1`)
```json
{
  "key":           "Forex",
  "estimationkey": "Forex_42",
  "uid":           42,
  "requestID":     -1,
  "synopsisID":    1,
  "estimation":    "EVICTED: inactive for TTL period. Re-register synopsis uid=42 to resume.",
  "param":         [],
  "noOfP":         1
}
```
Client must re-register on receiving `requestID=-1`.

---

## DataRouter: Hash-Routing Deep Dive

**This is the most important and non-obvious part of the system.**

DataRouter implements `FlatMapGroupsWithStateFunction<String, InputEvent, RoutingState, InputEvent>`, grouped by `dataSetKey`. Both requests and data events share the same Spark partition — no broadcast variable needed.

### RoutingState (per dataSetKey partition)
```java
Map<Integer, RoutingRegistration> registrations    // uid → {noOfP, requestID, dataSetKey}
Map<String, List<String>>         keysPerStream    // legacy, mostly unused
```
Active parallelism levels are derived on the fly from `registrations.values()` — no separate counter map.
TTL: 1 day (evicts if no events arrive for that key).

### Event ordering within each micro-batch
Events within a micro-batch are processed in arrival order — no sorting is applied.
If an ADD and DATA event arrive in the same batch, data may be processed before the
registration exists and that batch's data is missed for the new synopsis. If an ESTIMATE
arrives before DATA in the same batch, the result reflects state before the current batch.
Both are accepted one-batch imprecisions at transition points.

### GREEN path (noOfP == 1)
Request and data forwarded with original key unchanged. No fan-out.

### PURPLE path (noOfP > 1)
**On ADD request:**
```
Fan-out request to: "Forex_2_KEYED_0", "Forex_2_KEYED_1"
Registration stored in: registrations[uid] = {noOfP, requestID, dataSetKey}
```

**On data event:**
```java
// Derive active parallelism levels from registrations (distinct noOfP > 1 values)
// For each active level p:
int slot = Math.abs(datapoint.getStreamID().hashCode()) % p;
String routedKey = dataSetKey + "_" + p + "_KEYED_" + slot;
// Emit datapoint with routedKey
// Also emit with original key for any noOfP=1 synopses on same dataSetKey
```

**Example (noOfP=2, dataSetKey="Forex"):**
```
EURUSD → hash % 2 = 0 → Forex_2_KEYED_0  (Worker 0)
GBPUSD → hash % 2 = 1 → Forex_2_KEYED_1  (Worker 1)
USDJPY → hash % 2 = 1 → Forex_2_KEYED_1  (Worker 1)
```
Same streamID always routes to the same worker → consistent state partitioning.

**On ESTIMATE:** DataRouter looks up `noOfP` from the stored registration (same as DELETE — client does not need to supply it). Fans out to all N keyed partition keys. Each worker emits a partial `Estimation{noOfP=N}`. PathSplitter routes to ReduceAggregator. ReduceAggregator buffers until `count == noOfP`, then merges.

**On DELETE:** Fan out DELETE to all N keyed partition keys. Remove uid from `registrations`; the level stops being active automatically once no registration references it.

---

## Synopsis Algorithms

| ID | Class | Purpose | Result type | Min params |
|----|-------|---------|-------------|-----------|
| 1 | CountMin.java | Frequency / sum estimation | Long | 6 |
| 2 | Bloomfilter.java | Membership testing | Boolean | 5 |
| 3 | AMSsynopsis.java | Frequency moment F2 | Double | 5 |
| 4 | HyperLogLogSynopsis.java | Cardinality (distinct count) | Long | 4 |

### CountMin params
```
param[0] = keyField        e.g. "StockID"
param[1] = valueField      e.g. "price"  (or "null" to count occurrences)
param[2] = operationMode   e.g. "Queryable"
param[3] = epsilon         error bound, Double (e.g. "0.01")
param[4] = delta           confidence,  Double (e.g. "0.99")
param[5] = seed            random seed, Integer
```

### BloomFilter params
```
param[0] = keyField
param[3] = expectedInsertions   Integer
param[4] = falsePositiveRate    Double (e.g. "0.01")
```

### AMS params
```
param[0] = keyField
param[3] = buckets    Integer
param[4] = depth      Integer
```

### HyperLogLog params
```
param[1] = valueField   (field whose distinct values are counted)
param[3] = relativeStdDev   Double accuracy (e.g. "0.01" → ±1%)
```

### Merge strategy (PURPLE path)
- CountMin, AMS, HyperLogLog → `SimpleSumFunction` (sum partial values)
- BloomFilter → `SimpleORFunction` (bitwise OR of bit arrays)

### Kryo serialization / snapshotState
All synopsis types with transient third-party objects (BloomFilter, HyperLogLog, AMS) implement `snapshotState()` which serializes the internal object to a `byte[]` field before checkpointing. On restore, `ensureXxx()` reconstructs the object from bytes. CountMin is fully serializable and does not need this.

---

## Configuration Reference (SDEConfig.java)

| Field | Default | CLI Flag |
|-------|---------|---------|
| dataTopic | `data_topic` | `--data-topic` |
| requestTopic | `request_topic` | `--request-topic` |
| outputTopic | `estimation_topic` | `--output-topic` |
| kafkaBrokers | `localhost:9092` | `--kafka-brokers` |
| triggerInterval | `1 second` | `--trigger-interval` |
| checkpointLocation | `hdfs:///sde/checkpoints` | `--checkpoint-location` |
| safetyNetTtl | 7 days | `--safety-net-ttl` |
| routingStateTtl | 1 day | `--routing-state-ttl` |
| aggregationTimeout | 5 minutes | `--aggregation-timeout` |
| kafkaGroupId | `sde-spark` | `--kafka-group-id` |
| startingOffsets | `earliest` | `--starting-offsets` |
| failOnDataLoss | `false` | `--fail-on-data-loss` |
| kafkaProducerAcks | `all` | |
| kafkaProducerCompression | `lz4` | |
| kafkaProducerIdempotence | `true` | |

Kafka security (SASL/SSL): `--kafka-security-protocol`, `--kafka-sasl-mechanism`, `--kafka-sasl-jaas-config`, `--kafka-ssl-truststore-location`, `--kafka-ssl-truststore-password`

---

## Build & Run

```bash
# Build fat JAR
mvn clean package -DskipTests

# Start Kafka + Zookeeper
docker-compose up -d

# Run pipeline (local mode)
spark-submit --master local[4] \
  --class infore.sde.spark.SDESparkApp \
  target/sde-spark-1.0.0-SNAPSHOT.jar \
  --kafka-brokers localhost:9092 \
  --checkpoint-location file:///checkpoints/sde

# Windows convenience scripts
run-pipeline.cmd
run-producer.cmd
run-producer-streams.cmd

# Performance benchmark (2/4/6/8/16 workers, 2000 msg/sec, 60s)
./run-experiment-A.sh
```

---

## Key Dependencies (pom.xml)

| Dependency | Version | Purpose |
|-----------|---------|---------|
| spark-core_2.12 | 3.5.3 | Spark runtime |
| spark-sql_2.12 | 3.5.3 | Structured Streaming |
| spark-sql-kafka-0-10_2.12 | 3.5.3 | Kafka connector |
| jackson-databind | 2.15.3 | JSON serialization |
| stream (Clearspring) | 2.9.5 | CountMin, BloomFilter |
| streaminer | 1.1.1 | AMS, HyperLogLog |
| logback-classic | 1.4.14 | Logging |
| junit-jupiter | 5.10.1 | Tests |

Use Maven profile `local` to include Spark in compile scope for IDE development:
```bash
mvn compile -Plocal
```

---

## Tests

12 unit test classes under `src/test/java/infore/sde/spark/`:

| Area | Test class |
|------|-----------|
| Routing | RoutingStateTest, RequestRouterTest, InputEventTest |
| Processing | SynopsisProcessorTest, InputEventTest |
| Synopses | CountMinTest, SynopsisFactoryTest |
| Aggregation | ReduceFunctionTest, SimpleSumFunctionTest |
| Serialization | MessageSerializationTest, SynopsisSerializationTest |
| Config | SDEConfigTest |

```bash
mvn test
```

---

## State Management

### SynopsisProcessorState
- `Map<Integer, Synopsis> synopses` — maps `uid → synopsis instance`
- Kryo-serialized to HDFS/S3 checkpoint every micro-batch
- `snapshotForSerialization()` called before `state.update()` to capture transient objects

### AggregationState (ReduceAggregator)
- Buffers partial estimations keyed by `uid`
- Released when `count == noOfP` or on timeout (5 min default)

### TTL / Eviction
- `SynopsisProcessorState`: 7-day safety-net TTL
- `RoutingState`: 1-day TTL
- On eviction: `Estimation{requestID=-1}` emitted — client must re-register
- Checkpoint survives crash restart — Spark resumes from stored Kafka offset

---

## Metrics

### Spark Accumulators (visible in Spark UI)
```
sde.datapoints.processed
sde.requests.processed
sde.synopses.created / deleted
sde.estimations.emitted
sde.aggregations.completed
sde.parse.errors
sde.routing.fanouts
sde.state.timeouts
```

### Per-batch CSV (ThroughputListener)
Written to `results/` directory:
```
timestamp,batch_id,num_input_rows,input_rows_per_sec,processed_rows_per_sec,batch_duration_ms
```
Summary stats also written to `.summary.txt`.

---

## Architecture Presentation

**File:** `docs/spark/SDE-Spark-Architecture-Presentation-v3.pptx` (34 slides)
**Build script:** `docs/spark/build_presentation.py` (python-pptx 1.0.2)

Slide structure:
1. Title
2. Architecture overview (6 layers)
3. Message contracts (wire format)
4–9. Data ingestion flow (6 progressive steps)
10–15. DataRouter routing (6 progressive steps)
16. **DataRouter hash-routing deep dive** (RoutingState, hash formula, walkthrough)
17–23. GREEN path end-to-end (7 progressive steps)
24–30. PURPLE path end-to-end (7 progressive steps)
31. Synopsis algorithms (4 panels)
32. State management & checkpointing
33. Micro-batch timing & Spark partitioning
34. Performance & config reference

Rebuild: `python docs/spark/build_presentation.py`

**Important:** Uses only official python-pptx API — no lxml/XML injection. Previous attempts with custom animation XML caused PowerPoint repair dialog. Progressive slide duplication is used instead of click animations.

---

## Relationship to Original Flink SDE

The Flink SDE project lives at `C:\Users\dmeli\source\repos\SDE`.

- Same Kafka topic names: `data_topic`, `request_topic`, `estimation_topic`
- Same `Request` message schema (same field names, same semantics)
- Same synopsis algorithm IDs and parameter conventions
- Flink SDE used `fin_useCase` as dataSetkey in tests; SDE_Spark examples use `Forex`
- Flink SDE used topic names like `Rq_Fin`, `FAN` in test producers (not the defaults)
- SDE_Spark adds: Kryo checkpointing, hash-routing fan-out, ReduceAggregator, configurable TTLs
- SDE_Spark throughput: ~100K–1M events/sec (10x Flink) at cost of 1–5s latency (vs 10–100ms)

---

## Documentation Files

```
docs/spark/
├── spark-architecture.md               Full technical reference (777 lines)
├── spark-architecture-explained.md     High-level guide (169 lines)
├── spark-simple-architecture-guide.md  Simplified guide
├── spark-state-timeout-design.md       State TTL design decisions
└── e2e-validated-flow.md               End-to-end validated walkthrough with examples
```
