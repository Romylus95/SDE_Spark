# SDE_Spark

**Synopsis Data Engine** — A distributed system for maintaining and querying probabilistic data structures (synopses) over continuous data streams, built on Apache Spark 3.5.3 Structured Streaming.

## Overview

SDE_Spark processes real-time data streams via Apache Kafka, maintains in-memory synopsis data structures, and responds to estimation queries with sub-linear space and time complexity. The system supports parallel synopsis processing across multiple partitions with automatic result aggregation.

### Synopsis Algorithms

| ID | Algorithm | Use Case |
|----|-----------|----------|
| 1 | CountMin Sketch | Frequency estimation (e.g., total price per stock) |
| 2 | Bloom Filter | Membership testing (e.g., "has stock X been seen?") |
| 3 | AMS Sketch | Frequency moment estimation |
| 4 | HyperLogLog | Cardinality estimation (e.g., distinct stock count) |

## Architecture

The pipeline follows a 6-layer architecture:

```
Kafka ──> [Layer 1: Ingestion] ──> [Layer 2: Routing] ──> [Layer 3: Synopsis Processing]
              │                                                      │
              │                                              ┌───────┴───────┐
              │                                          GREEN path     PURPLE path
              │                                          (noOfP=1)      (noOfP>1)
              │                                              │               │
              │                                              │    [Layer 4: Path Split]
              │                                              │               │
              │                                              │    [Layer 5: Aggregation]
              │                                              │               │
              │                                              └───────┬───────┘
              │                                                      │
         Kafka <── [Layer 6: Output] <──────────────────────────────┘
```

| Layer | Component | Description |
|-------|-----------|-------------|
| 1 | KafkaIngestionLayer | Consumes JSON from Kafka, deserializes to typed POJOs |
| 2 | DataRouter | Stateful request registration + data fan-out routing |
| 3 | SynopsisProcessor | Synopsis lifecycle: ADD, DATA, ESTIMATE, DELETE |
| 4 | PathSplitter | Routes by parallelism: GREEN (direct) or PURPLE (aggregate) |
| 5 | ReduceAggregator | Merges partial results from parallel partitions |
| 6 | KafkaOutputLayer | Serializes estimations to JSON, writes to Kafka |

## Technology Stack

- **Apache Spark** 3.5.3 (Structured Streaming)
- **Apache Kafka** (message broker)
- **Java 17**
- **Kryo** serialization
- **Jackson** 2.15.3 (JSON processing)
- **stream-lib / streaminer** (probabilistic data structures)
- **JUnit 5** + AssertJ (testing)
- **Maven** (build)

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.x
- Docker (for Kafka)

### 1. Start Kafka

```bash
docker-compose up -d
```

### 2. Build

```bash
mvn clean package -DskipTests
```

### 3. Run

```bash
spark-submit --master local[4] \
  --class infore.sde.spark.SDESparkApp \
  target/sde-spark-1.0.0-SNAPSHOT.jar \
  --kafka-brokers localhost:9092
```

### 4. Run Tests

```bash
mvn test
```

## Configuration

Configuration via CLI arguments, properties file, or both (CLI overrides file):

```bash
spark-submit ... sde-spark-1.0.0-SNAPSHOT.jar \
  --config /path/to/sde-spark.properties \
  --kafka-brokers localhost:9092 \
  --data-topic data_topic \
  --request-topic request_topic \
  --output-topic estimation_topic \
  --trigger-interval "2 seconds" \
  --checkpoint-location hdfs:///sde/checkpoints
```

See `src/main/resources/sde-spark.properties.example` for all available options including Kafka security (SASL/SSL), producer tuning, and state timeouts.

## Kafka Message Contract

**Data** (`data_topic`):
```json
{"dataSetkey": "Forex", "streamID": "EURUSD", "values": {"StockID": "AAPL", "price": "185.50"}}
```

**Request** (`request_topic`):
```json
{"dataSetkey": "Forex", "requestID": 1, "synopsisID": 1, "uid": 10,
 "streamID": "ALL", "param": ["StockID","price","Queryable","0.01","0.99","42"], "noOfP": 1}
```

**Estimation** (`estimation_topic`):
```json
{"key": "Forex", "estimationkey": "10", "uid": 10, "requestID": 3,
 "synopsisID": 1, "estimation": "549.0", "param": ["AAPL"], "noOfP": 1}
```

Request operations: `1` = ADD, `2` = DELETE, `3` = ESTIMATE

## Processing Paths

- **GREEN Path** (`noOfP = 1`): Single synopsis instance per dataset key. Estimations go directly to output.
- **PURPLE Path** (`noOfP > 1`): Data is hash-partitioned across N parallel synopsis instances. Partial estimations are merged by the ReduceAggregator before output.

## Project Structure

```
src/main/java/infore/sde/spark/
├── SDESparkApp.java              # Entry point
├── config/                       # SDEConfig (CLI + properties file)
├── ingestion/                    # Layer 1: Kafka consumer
├── routing/                      # Layer 2: DataRouter, RoutingState
├── processing/                   # Layer 3: SynopsisProcessor, InputEvent
├── aggregation/                  # Layers 4-5: PathSplitter, ReduceAggregator
├── output/                       # Layer 6: Kafka producer
├── messages/                     # Datapoint, Request, Estimation POJOs
├── synopses/                     # Synopsis algorithms + factory
├── reduceFunctions/              # Sum and OR aggregation functions
├── metrics/                      # PipelineMetrics, ThroughputListener
└── integration/                  # Test apps and load test producer
```

## Testing

51 unit tests covering:
- Synopsis algorithm correctness (all 4 types)
- Serialization round-trips (Kryo compatibility)
- Routing state management
- Configuration loading (CLI + file)
- Aggregation functions (Sum, OR)
- Message JSON serialization (Kafka contract)

## Experimental Evaluation

Preliminary results from Experiment B (Throughput vs Ingestion Rate):

| Target Rate | Avg Processed (rows/sec) | Peak (rows/sec) | Avg Batch (ms) |
|-------------|--------------------------|------------------|----------------|
| 100 msg/s | 86.4 | 107.7 | 6,805 |
| 500 msg/s | 447.7 | 524.3 | 6,786 |
| 1,000 msg/s | 959.6 | 1,035.5 | 6,356 |

Key finding: near-linear throughput scaling with constant batch duration.

See `results/` for full data and `docs/spark/SDE-Spark-Implementation-Report.docx` for the detailed report.

## Documentation

- `docs/spark/SDE-Spark-Implementation-Report.docx` — Full implementation report (19 sections)
- `docs/spark/spark-simple-architecture-guide.md` — Architecture guide
- `docs/spark/e2e-validated-flow.md` — Validated GREEN + PURPLE flows with real numbers
- `docs/spark/SDE-Spark-Architecture-Presentation.pptx` — Presentation slides
