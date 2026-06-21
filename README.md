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
