# SDE Spark Edition — Architecture & Flow Guide

## Document Information

| Field | Value |
|-------|-------|
| **Document Type** | Architecture Guide — Simple Explanation with Full Technical Detail |
| **Audience** | Developer onboarding, implementation reference |
| **Status** | Approved for Implementation |
| **Related Docs** | `spark-architecture.md`, `spark-state-timeout-design.md` |

---

## Part 1 — What Is This System and Why Does It Exist?

### The Problem

Imagine a candy shop with **millions of customers walking in every single second**. The owner wants to know things like:

- *"How many times did customer AAPL buy something today?"*
- *"Has this customer ever visited before?"*
- *"How many different customers visited today?"*

The naive solution — write down every single customer event — would fill a warehouse of notebooks. It is too slow and too expensive at millions of events per second.

**The SDE solves this with magic notebooks called synopses (probabilistic data structures).** Instead of recording every event, a synopsis keeps a tiny mathematical summary that can answer questions with a small, acceptable margin of error. A HyperLogLog counting 1 billion unique visitors is the same 12 KB size as one that has seen only 100. The sketch never grows as more data flows through it.

### The Two Inputs

Everything that enters the system is one of two things:

**1 — Data events** (the stream of things happening)
```json
{ "dataSetkey": "Forex", "streamID": "EURUSD", "values": { "StockID": "AAPL", "price": "182.50" } }
{ "dataSetkey": "Forex", "streamID": "EURUSD", "values": { "StockID": "GOOG", "price": "141.20" } }
{ "dataSetkey": "Forex", "streamID": "EURUSD", "values": { "StockID": "AAPL", "price": "182.55" } }
```

**2 — Request commands** (the boss telling the system what to do)
```json
{ "requestID": 1, "synopsisID": 1, "uid": 42, "noOfP": 2, ... }   ← ADD: create a synopsis
{ "requestID": 3, "synopsisID": 1, "uid": 42, ... }               ← ESTIMATE: give me the answer
{ "requestID": 2, "synopsisID": 1, "uid": 42, ... }               ← DELETE: remove synopsis
```

Both arrive through **Apache Kafka** — the system's mailbox. Kafka is a message queue that receives, stores, and delivers messages reliably at massive scale.

---

## Part 2 — The Building with 6 Rooms

Think of the entire Spark pipeline as a **building with 6 rooms**. Every message walks through all rooms in order. Each room has exactly one job.

```
📬 Kafka (Data Topic + Request Topic)
        ↓
┌─────────────────────────────┐
│  Room 1: Receptionist       │  Read letters, translate to objects
└──────────────┬──────────────┘
               ↓
┌─────────────────────────────┐
│  Room 2: Manager            │  Decide who gets what, make copies
└──────────────┬──────────────┘
               ↓
┌─────────────────────────────┐
│  Room 3: Workers ⭐         │  Magic notebooks — real work happens here
│  (state stored in memory)   │
└──────────────┬──────────────┘
               ↓
┌─────────────────────────────┐
│  Room 4: Traffic Light      │  1 worker? go now. Many workers? wait.
└──────────────┬──────────────┘
               ↓ (purple path only)
┌─────────────────────────────┐
│  Room 5: Combining Table    │  Merge partial answers into one final answer
└──────────────┬──────────────┘
               ↓
┌─────────────────────────────┐
│  Room 6: Delivery Truck     │  Serialize to JSON, write to Kafka output
└──────────────┬──────────────┘
               ↓
📬 Kafka (Estimation Output Topic)
        ↓
👤 Client reads the answer
```

---

## Part 3 — Each Room Explained in Full Detail

---

### Room 1 — Ingestion & Parsing (The Receptionist)

**Simple explanation:** The mailman drops off crumpled letters. The receptionist opens each one and rewrites it neatly so the rest of the building can understand it.

**Technical reality:** Kafka delivers raw bytes. This layer deserializes the JSON string into a strongly-typed Java POJO.

**What comes in:**
```
Raw Kafka record bytes:
'{"dataSetkey":"Forex","streamID":"EURUSD","values":{"StockID":"AAPL","price":"182.50"}}'
```

**What goes out:**
```java
Datapoint {
    dataSetKey = "Forex",
    streamID   = "EURUSD",
    values     = JsonNode { "StockID": "AAPL", "price": "182.50" }
}
```

**For requests:**
```
Raw bytes: '{"dataSetkey":"Forex","requestID":1,"synopsisID":1,"uid":42,"noOfP":2,...}'
```
```java
Request {
    dataSetKey = "Forex",
    requestID  = 1,       // ADD
    synopsisID = 1,       // CountMin
    uid        = 42,
    noOfP      = 2,
    param      = ["StockID", "price", "Queryable", "0.01", "4"]
}
```

**Spark implementation:** `readStream.format("kafka")` + Jackson `ObjectMapper` deserialization inside a `map()` transformation.

**Error handling:** Malformed JSON that cannot be parsed is logged at WARN level and dropped. A bad message must never crash the pipeline.

---

### Room 2 — Routing & Registration (The Manager)

**Simple explanation:** The manager has a whiteboard showing which workers are responsible for what. When a message comes in, the manager decides who gets it — and sometimes makes multiple copies of the same message for multiple workers.

**Technical reality:** This room solves two problems.

#### Problem A — The Two-Stream Problem (Union + Discriminator Pattern)

Flink had a native `CoFlatMapFunction` that could natively receive two typed streams. Spark does not. The solution: **tag every message before merging both streams into one**.

```java
// Tag each stream with a type marker
Dataset<SynopsisInput> taggedData     = dataStream.map(d -> new DataEvent(d));
Dataset<SynopsisInput> taggedRequests = requestStream.map(r -> new RequestEvent(r));

// Merge into one unified stream
Dataset<SynopsisInput> combined = taggedData.union(taggedRequests);

// Group by key — all messages for "Forex" go to the same processor
combined.groupByKey(e -> e.getDataSetKey())
        .flatMapGroupsWithState(...);
```

Inside the processor, the tag is used to dispatch:
```java
if (event instanceof DataEvent de)    → handle as data
if (event instanceof RequestEvent re) → handle as request
```

#### Problem B — Request Fan-Out

When a client says `noOfP=2`, it means the synopsis should run across 2 parallel workers. The manager must **expand 1 request into 2 copies**, each stamped with its partition slot.

```
Request(uid=42, noOfP=2, dataSetKey="Forex")
                    ↓
      ┌─────────────┴─────────────┐
      ↓                           ↓
Request(key="Forex_2_KEYED_0")  Request(key="Forex_2_KEYED_1")
```

#### Problem C — Data Routing

When data arrives, the manager checks its **registration table** (stored in GroupState):

```
KeysPerStream["EURUSD"] = ["Forex_2_KEYED_0", "Forex_2_KEYED_1"]
KeyedParallelism[uid=42] = 2
```

Then hashes the data key to pick the right partition:
```
hash("AAPL") % 2 = 0  →  route to "Forex_2_KEYED_0"  (Worker A)
hash("GOOG") % 2 = 1  →  route to "Forex_2_KEYED_1"  (Worker B)
hash("AAPL") % 2 = 0  →  route to "Forex_2_KEYED_0"  (Worker A, again — always consistent)
```

The same key always goes to the same worker. This consistency is essential for counting accuracy.

---

### Room 3 — Synopsis Processing (The Workers with Magic Notebooks) ⭐

**Simple explanation:** Each worker has a magic notebook. The boss opens notebooks, data gets recorded in them, and queries read from them. All notebooks are kept in a fireproof safe.

**Technical reality:** This is the core of the entire system. Implemented as `flatMapGroupsWithState` with `GroupStateTimeout.ProcessingTimeTimeout()`.

#### The Four Magic Notebooks (Algorithms)

| Notebook | synopsisID | Question | Memory used |
|----------|-----------|----------|-------------|
| **CountMin Sketch** | 1 | "How many times did X appear?" | ~64 KB |
| **Bloom Filter** | 2 | "Have we seen X before? (yes/no)" | ~1.2 MB |
| **HyperLogLog** | 4 | "How many *different* items appeared?" | ~12 KB |
| **AMS Sketch** | 3 | "What is the statistical distribution?" | ~few KB |

All four extend the same abstract base class:
```java
abstract class Synopsis implements Serializable {
    abstract void   add(Object key);           // record a data point
    abstract Object estimate(Object key);      // answer a query
    abstract Estimation estimate(Request rq);  // answer with full metadata
    abstract Synopsis merge(Synopsis other);   // combine two partial synopses
}
```

#### The Three Operations

**Operation 1 — ADD (requestID=1): Open a new notebook**

```java
// SynopsisFactory creates the correct algorithm
Synopsis sketch = SynopsisFactory.create(synopsisID=1, uid=42, params);
// → new CountMin(epsilon=0.01, confidence=0.99, seed=4)

state.get().synopses.put(42, sketch);
state.update(state.get());
// No output emitted. Just registration.
```

**Operation 2 — DATA arrives: Write in the notebook**

```java
// For every active synopsis on this stream
for (Synopsis sketch : state.get().synopses.values()) {
    String keyField   = sketch.getKeyIndex();    // "StockID" from params
    String valueField = sketch.getValueIndex();  // "price" from params
    sketch.add(datapoint.values.get(keyField));  // sketch.add("AAPL")
}
// No output emitted. Just updating the sketch.
```

**Operation 3 — ESTIMATE (requestID=3): Read the notebook**

```java
Synopsis sketch = state.get().synopses.get(uid=42);
Estimation result = sketch.estimate(request);
// result.estimation = 2  (AAPL appeared 2 times)
// result.noOfP = 2       (signal that 2 partials exist)

output.add(result);  // emit the partial answer
```

#### State Storage — In-Memory (Default) with Checkpoint Persistence

All notebook state is stored **in-memory** (Java HashMap) using Spark's default `HDFSBackedStateStore`. This is the simplest and fastest option — no extra dependencies, no disk I/O overhead during normal processing.

**Why in-memory and not RocksDB?**

| Property | In-Memory (Spark's default) | RocksDB (available as upgrade) |
|----------|----------------------------|-------------------------------|
| Speed | Fastest (direct HashMap access) | Fast (~microseconds, SSD) |
| Size limit | Executor memory (~GB) | Disk (~TB) |
| Setup | Zero config | Requires RocksDB provider config |
| Best for | Small-medium state (our use case) | Very large state (10K+ synopses) |

For our current workload, in-memory state is more than sufficient. If state grows beyond executor memory in the future, switching to RocksDB is a single config change:
```bash
--conf spark.sql.streaming.stateStore.providerClass=
  org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
```

**Key implication:** Regardless of whether state is in-memory or in RocksDB, Spark serializes state during checkpointing. Every Synopsis subclass **must** implement `Serializable` and provide a stable serialization format. This is the highest-risk task in the entire project (flagged as the Serialization Spike epic — must be completed first).

#### The Warehouse Backup — Checkpointing to HDFS/S3

In-memory state handles fast access but is lost if the process crashes. Spark periodically takes a snapshot of all state and writes it to **HDFS or S3** — a remote, redundant storage system.

```bash
--conf spark.sql.streaming.checkpointLocation=hdfs:///sde/checkpoints
```

```
Every micro-batch interval (e.g. every 1 second):
    In-memory state (HashMap)
            │
            │  only the changes since last checkpoint (incremental)
            ↓
    HDFS / S3  ←  survives full node destruction
```

**On recovery after a crash:**
```
1. Spark job restarts
2. Reads last checkpoint from HDFS/S3
3. Restores all in-memory state from checkpoint
4. Resumes processing from the Kafka offset stored in the checkpoint
5. Continues as if nothing happened
```

This is exactly-once semantics — no event is counted twice, no event is missed.

**In the old Flink version:** all state was a `HashMap` in JVM heap. A crash meant total loss. Every synopsis had to be re-registered by clients, and all accumulated sketch data was gone permanently.

#### The Janitor — State Timeout

Over time, workers accumulate notebooks that nobody ever uses again. Clients crash, forget to send DELETE, or finish a one-time experiment. Without cleanup, the fireproof safe fills up forever.

**How the janitor works:**

A synopsis can be registered with an optional TTL (time-to-live):
```json
{ "requestID": 1, "synopsisID": 1, "uid": 42, "noOfP": 2, "ttl": "24h" }
```

The rule: **if nobody touches this notebook for 24 hours straight, the janitor removes it.**

Crucially, the timer resets on every activity — so an actively used synopsis is never evicted:
```
ADD request          → timer set to 24h from now
data arrives         → timer resets to 24h from now
ESTIMATE request     → timer resets to 24h from now
data arrives         → timer resets to 24h from now
  [24 hours of silence]
EVICTION             → janitor sends a notice, removes the state
```

**Eviction is never silent.** Before removing the notebook, the janitor sends a notification to the Kafka output topic:
```json
{
  "uid": 42,
  "requestID": -1,
  "estimation": "EVICTED: inactive for 24h. Re-register synopsis uid=42 to resume.",
  "synopsisID": 1
}
```

The client can re-register if the synopsis is still needed.

**For synopses with no TTL:** a global safety-net (default: 7 days) acts as last-resort cleanup for completely abandoned state. Synopses that are actively used never hit this limit.

```java
// In SynopsisProcessor.call()
if (state.hasTimedOut()) {
    List<Estimation> notices = buildEvictionNotices(key, state.get());
    state.remove();
    return notices.iterator();
}

// After every batch of events — reset the activity clock
Duration timeout = (state.get().getTtl() != null)
    ? state.get().getTtl()
    : globalConfig.getSafetyNetTtl();   // 7 days default
state.setTimeoutDuration(timeout.toMillis() + " milliseconds");
```

---

### Room 4 — Path Splitting (The Traffic Light)

**Simple explanation:** One question — does this result need combining? Green means send it straight out. Purple means go to the combining table first.

**Technical reality:** Two simple `filter()` operations. No state, no complexity.

```java
Dataset<Estimation> greenPath  = estimations.filter(e -> e.getNoOfP() == 1);
Dataset<Estimation> purplePath = estimations.filter(e -> e.getNoOfP() > 1);
```

| Path | Condition | Meaning |
|------|-----------|---------|
| **GREEN** | `noOfP == 1` | Synopsis ran on a single worker. Answer is complete. Send now. |
| **PURPLE** | `noOfP > 1` | Synopsis ran across multiple workers. Partial answers need merging. |

---

### Room 5 — Aggregation (The Combining Table)

**Simple explanation:** All partial answers for the same question arrive here. The table waits until it has collected all of them, then adds them together into one final answer.

**Technical reality:** `flatMapGroupsWithState` grouped by `uid`, collecting partial results until `count == noOfP`.

```java
// Wait until we have noOfP partial results for uid=42
if (partials.size() == request.getNoOfP()) {
    Synopsis merged = partials.get(0).getSynopsis();
    for (int i = 1; i < partials.size(); i++) {
        merged = merged.merge(partials.get(i).getSynopsis());
    }
    output.add(merged.estimate(originalRequest));
    state.remove();   // clean up — aggregation complete
}
```

**Merge strategy per algorithm:**

| Algorithm | Merge rule | Why |
|-----------|-----------|-----|
| CountMin | Sum the count tables element-by-element | Each partition counted a subset; totals add up |
| BloomFilter | Bitwise OR the bit arrays | A bit is "on" if any partition saw that element |
| HyperLogLog | Union the register arrays | Standard HLL merge procedure |
| AMS Sketch | Sum the sketch arrays | Same reason as CountMin |

**Key improvement over Flink:** Flink required a `GReduceFlatMap` with `setParallelism(1)` — a single-threaded global bottleneck. Everything had to queue through one node. Spark's `groupByKey(uid)` distributes this reduction across the cluster. No queue, no bottleneck.

---

### Room 6 — Output (The Delivery Truck)

**Simple explanation:** Take the final answer, put it in a letter (JSON), drop it in the output mailbox.

**Technical reality:** Spark Kafka sink.

```java
estimations
    .selectExpr("CAST(uid AS STRING) AS key",
                "to_json(struct(*)) AS value")
    .writeStream()
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("topic", "estimation_topic")
    .outputMode("append")
    .start();
```

**Final message on output topic:**
```json
{
  "key":           "Forex_42",
  "estimationkey": "Forex_42",
  "streamID":      "EURUSD",
  "uid":           42,
  "requestID":     3,
  "synopsisID":    1,
  "estimation":    2,
  "param":         ["StockID", "price", "Queryable", "0.01", "4"],
  "noOfP":         2
}
```

The Kafka wire format is **byte-for-byte identical to the Flink version**. Any client that worked against the Flink SDE works against the Spark SDE without modification.

---

## Part 4 — Complete End-to-End Flow Examples

---

### Example 1 — GREEN PATH (single worker, direct output)

**Scenario:** Count how many times AAPL appears. One worker, no merging needed.

```
👤 Client sends ADD request:
   { requestID:1, synopsisID:1, uid:10, noOfP:1, param:["StockID","price",...] }

─── Room 1 ──────────────────────────────────────────────────────
   Raw JSON → Request { uid=10, noOfP=1, synopsisID=1 }

─── Room 2 ──────────────────────────────────────────────────────
   noOfP=1 → no fan-out, 1 copy only
   Tagged as RequestEvent, merged into combined stream
   Keyed by "Forex"

─── Room 3 ──────────────────────────────────────────────────────
   RequestEvent (ADD) → SynopsisFactory.create(1, 10, params)
   State: { uid=10 → CountMin(empty) }  saved to state
   No output emitted.

─── DATA FLOWS ──────────────────────────────────────────────────
   "AAPL" → Room 1 → Room 2 (route to slot 0) → Room 3
             CountMin.add("AAPL")   → state updated
   "GOOG" → Room 1 → Room 2 (route to slot 0) → Room 3
             CountMin.add("GOOG")   → state updated
   "AAPL" → Room 1 → Room 2 → Room 3
             CountMin.add("AAPL")   → state updated

─── Client sends ESTIMATE request ───────────────────────────────
   { requestID:3, uid:10, noOfP:1 }

─── Room 3 ──────────────────────────────────────────────────────
   Reads CountMin from state
   Returns Estimation { uid=10, estimation=2, noOfP=1 }

─── Room 4 ──────────────────────────────────────────────────────
   noOfP=1 → 🟢 GREEN PATH

─── Room 5 ──────────────────────────────────────────────────────
   Skipped entirely (green path bypasses combining table)

─── Room 6 ──────────────────────────────────────────────────────
   Serializes to JSON, writes to Kafka output topic

👤 Client reads: { "estimation": 2 }  ✅
   "AAPL appeared approximately 2 times"
```

---

### Example 2 — PURPLE PATH (multiple workers, merging required)

**Scenario:** Count unique Forex tickers. 2 parallel workers for higher throughput.

```
👤 Client sends ADD request:
   { requestID:1, synopsisID:4, uid:42, noOfP:2, param:["StockID",...] }
   (synopsisID=4 → HyperLogLog)

─── Room 1 ──────────────────────────────────────────────────────
   Raw JSON → Request { uid=42, noOfP=2, synopsisID=4 }

─── Room 2 ──────────────────────────────────────────────────────
   noOfP=2 → fan-out to 2 copies:
     Request { key="Forex_2_KEYED_0" }
     Request { key="Forex_2_KEYED_1" }

─── Room 3 (runs twice, once per partition) ──────────────────────
   Partition 0: creates HyperLogLog instance → saved to state
   Partition 1: creates HyperLogLog instance → saved to state

─── DATA FLOWS ──────────────────────────────────────────────────
   "AAPL" → hash % 2 = 0 → Worker 0 → hll.add("AAPL")
   "GOOG" → hash % 2 = 1 → Worker 1 → hll.add("GOOG")
   "MSFT" → hash % 2 = 0 → Worker 0 → hll.add("MSFT")
   "AAPL" → hash % 2 = 0 → Worker 0 → hll.add("AAPL")  ← duplicate
   "TSLA" → hash % 2 = 1 → Worker 1 → hll.add("TSLA")

─── Client sends ESTIMATE request ───────────────────────────────
   { requestID:3, uid:42, noOfP:2 }

─── Room 3 ──────────────────────────────────────────────────────
   Worker 0 reads HLL → Estimation { uid=42, partial=hll0, noOfP=2 }
   Worker 1 reads HLL → Estimation { uid=42, partial=hll1, noOfP=2 }
   Both emitted downstream.

─── Room 4 ──────────────────────────────────────────────────────
   noOfP=2 → 🟣 PURPLE PATH (both partials)

─── Room 5 ──────────────────────────────────────────────────────
   Collect partials for uid=42:
     arrival 1: hll0 (from Worker 0) → buffer: [hll0], have 1, need 2
     arrival 2: hll1 (from Worker 1) → buffer: [hll0, hll1], have 2 ✅
   Merge: HyperLogLog.union(hll0, hll1)
   Final cardinality: 4 unique tickers (AAPL, GOOG, MSFT, TSLA)
   Duplicate AAPL correctly not counted twice.

─── Room 6 ──────────────────────────────────────────────────────
   Writes final Estimation to Kafka

👤 Client reads: { "estimation": 4 }  ✅
   "Approximately 4 unique stock tickers observed"
```

---

### Example 3 — State Timeout (The Janitor)

**Scenario:** A developer creates a synopsis for a quick test, then forgets to clean up.

```
👤 Dev sends ADD request with a short TTL for testing:
   { requestID:1, synopsisID:2, uid:99, noOfP:1, ttl:"1h" }
   (synopsisID=2 → BloomFilter)

─── Room 3 ──────────────────────────────────────────────────────
   Creates BloomFilter → saved to state
   Activity timer set to: now + 1 hour

─── DATA FLOWS for 30 minutes ───────────────────────────────────
   Each data event → BloomFilter.add(value)
   Each data event → timer resets to: now + 1 hour

─── Dev closes their laptop, forgets about it ───────────────────

─── 1 hour passes with zero activity ────────────────────────────

─── Room 3 — Janitor fires ──────────────────────────────────────
   state.hasTimedOut() == true
   Janitor emits eviction notice:
   {
     "uid": 99,
     "requestID": -1,
     "estimation": "EVICTED: inactive for 1h. Re-register to resume.",
     "synopsisID": 2
   }
   state.remove() → state entry deleted

─── Room 6 ──────────────────────────────────────────────────────
   Eviction notice written to Kafka output topic

👤 Dev (if monitoring) reads: "EVICTED: uid=99"
   Can re-register if still needed. Otherwise state stays clean.
```

---

### Example 4 — Recovery After Crash

**Scenario:** The Spark executor crashes mid-stream. What happens?

```
State before crash:
  State:    { uid=42 → HyperLogLog (seen 1.5M unique items) }
  Last checkpoint written to HDFS: 10 seconds ago
  Last processed Kafka offset: partition=3, offset=88200

─── Executor crashes ────────────────────────────────────────────

─── Spark detects failure, restarts executor ─────────────────────

─── Recovery ────────────────────────────────────────────────────
  1. Reads checkpoint from HDFS
  2. Restores state: { uid=42 → HyperLogLog (seen 1.5M items) }
  3. Resumes reading Kafka from offset 88200 (no events missed)
  4. Processing continues as if crash never happened

─── Compare to Flink version ────────────────────────────────────
  Flink crash:
    HashMap was in JVM heap → gone
    HyperLogLog history: lost
    Client must re-register uid=42 from scratch
    All 1.5M items must be re-processed or state is wrong forever
```

---

## Part 5 — Performance Characteristics

| Metric | Flink SDE | Spark SDE | Winner |
|--------|-----------|-----------|--------|
| Latency | ~10–100 ms (event-at-a-time) | ~1–5 s (micro-batch) | Flink |
| Throughput | 10K–100K events/sec | 100K–1M events/sec | Spark |
| State durability | None — lost on crash | Checkpointed to HDFS/S3 | Spark |
| Max state size | JVM heap (~GB) | Disk (~TB, effectively unlimited) | Spark |
| Fault recovery | Restart from zero | Resume from checkpoint | Spark |
| Exactly-once | Requires config | Built-in | Spark |
| Code complexity | Lower | Higher | Flink |

**Accepted trade-off:** Latency increases from milliseconds to seconds. This is explicitly accepted in the PRD in exchange for 10x throughput and production-grade durability.

---

## Part 6 — The Full System at a Glance

```
┌─────────────────────────────────────────────────────────────────────┐
│                        KAFKA INPUT                                  │
│  data_topic (events)           request_topic (commands)             │
└──────────────┬──────────────────────────┬───────────────────────────┘
               │                          │
               ▼                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 1 — Ingestion & Parsing                                      │
│  JSON bytes → Datapoint / Request POJOs                             │
└──────────────────────────────┬──────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 2 — Routing & Registration                                   │
│  Tag streams → union → groupByKey                                   │
│  Fan-out requests (noOfP copies)                                    │
│  Hash data to correct partition slot                                │
└──────────────────────────────┬──────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 3 — Synopsis Processing  ⭐                                  │
│  flatMapGroupsWithState (ProcessingTimeTimeout)                     │
│  ADD    → SynopsisFactory.create() → store in GroupState            │
│  DATA   → synopsis.add(value) → update GroupState                  │
│  ESTIMATE → synopsis.estimate() → emit Estimation                  │
│  DELETE → state.remove()                                           │
│  TIMEOUT → eviction notice → state.remove()                        │
│                                                                     │
│  State backend: In-memory (default) — upgradable to RocksDB          │
│  Checkpoints:   HDFS / S3  (periodic snapshots)                     │
└──────────────────────────────┬──────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 4 — Path Splitting                                           │
│  filter(noOfP == 1) → GREEN PATH ──────────────────────────┐       │
│  filter(noOfP  > 1) → PURPLE PATH ─────────────────────┐   │       │
└────────────────────────────────────────────────────────┼───┼───────┘
                                                         │   │
                         ┌───────────────────────────────┘   │
                         ▼                                   │
┌─────────────────────────────────────┐                      │
│  LAYER 5 — Aggregation              │                      │
│  Collect noOfP partials for uid     │                      │
│  Merge via algorithm-specific rule  │                      │
│  Emit single final Estimation       │                      │
└───────────────────┬─────────────────┘                      │
                    │                                         │
                    └──────────────┬──────────────────────────┘
                                   ↓
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 6 — Output                                                   │
│  Serialize Estimation to JSON → Kafka estimation_topic              │
└──────────────────────────────┬──────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────────┐
│                       KAFKA OUTPUT                                  │
│                   estimation_topic                                  │
└─────────────────────────────────────────────────────────────────────┘
                               ↓
                          👤 Client
```

---

## Part 7 — Key Files Reference

| File | Location | Purpose |
|------|----------|---------|
| `spark-architecture.md` | `docs/spark/` | Full technical architecture spec for SDE_Spark |
| `spark-architecture-explained.md` | `docs/spark/` | Plain-language layer-by-layer explanation |
| `spark-state-timeout-design.md` | `docs/spark/` | State timeout detailed design and implementation |
| `spark-simple-architecture-guide.md` | `docs/spark/` | This document |
| `spark-architecture-diagram.png` | `docs/spark/` | Visual architecture diagram |
| `prd.md` | `docs/` | Full product requirements (Spark Edition v1.1) |
| `architecture.md` | `docs/` | Original Flink architecture (reference) |
