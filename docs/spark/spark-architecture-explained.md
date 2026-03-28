# SDE Spark Architecture — How It Works

## The Big Picture

The SDE is like a **factory assembly line** for stream analytics. Raw data flows in one end, gets processed by probabilistic algorithms (synopses), and query results come out the other end. The whole system communicates through **Kafka** — a message bus that acts like a postal service between components.

There are two inputs:
- **Data** — the actual stream events (stock prices, user clicks, sensor readings)
- **Requests** — management commands ("create a counter", "give me the count", "delete that counter")

And one output:
- **Estimations** — the answers to queries ("there are approximately 1.5 million unique users")

---

## Layer by Layer

### Layer 1: Ingestion & Parsing
**"The Mailroom"**

Raw JSON text arrives from Kafka. This layer's only job is to **open the envelope and read it**:
- Data messages become `Datapoint` objects (with a key, stream name, and values)
- Request messages become `Request` objects (with an operation type, algorithm choice, and parameters)

Think of it as a receptionist who sorts incoming mail into two piles: "data" and "requests."

---

### Layer 2: Routing & Registration
**"The Switchboard"**

This is where the system figures out **who needs what**.

**RequestRouter:** When you send a request like "create a CountMin sketch with parallelism 4," this layer **copies that request 4 times** — one for each parallel worker. It's like a manager assigning the same task to 4 team members so they can each handle a portion.

**DataRouter:** When a data point arrives, this layer checks: "which synopses are listening to this data stream?" and **forwards copies** to each one. If you have a CountMin AND a HyperLogLog both tracking the "Forex" stream, both get the data.

The key concept: routing keeps a **registration table** — a list of which synopses exist and where they live. When you ADD a synopsis, it gets registered. When you DELETE, it gets removed.

---

### Layer 3: Core Synopsis Processing
**"The Brain"**

This is the heart of the entire system. It manages the **actual synopsis algorithms** — the probabilistic data structures that give SDE its power.

It handles four operations:
1. **ADD request** — Creates a new synopsis instance (e.g., "spin up a HyperLogLog to count unique users")
2. **Data arrives** — Feeds the data into all active synopses for that stream (e.g., "this user visited the site — tell the HyperLogLog")
3. **ESTIMATE request** — Queries a synopsis for its current answer (e.g., "how many unique users so far?" — "approximately 1,543,287")
4. **DELETE request** — Removes a synopsis instance

The four algorithms available in v1.0:

| Algorithm | What it does | Analogy |
|-----------|-------------|---------|
| **CountMin** | Counts how often things appear | "How many times did user X visit?" |
| **BloomFilter** | Tests if something was seen before | "Have we seen this credit card number?" (yes/no) |
| **HyperLogLog** | Counts unique items | "How many distinct users visited today?" |
| **AMS Sketch** | Measures statistical properties | "What's the distribution shape of these values?" |

All state is stored **in-memory** (Java HashMap) for fast access and periodically **checkpointed** to HDFS/S3. This means if the system crashes, it can recover from the last checkpoint without losing all its data — a major improvement over the Flink version which lost everything on restart. If state grows beyond memory limits in the future, Spark can be configured to use RocksDB (an embedded key-value store on local SSD) as a drop-in replacement.

---

### Layer 4: Path Splitting
**"The Traffic Light"**

When an estimation result comes out of Layer 3, this layer asks one simple question: **"Does this result need merging?"**

- If `noOfP == 1` (single partition) — **GREEN PATH** — the result is complete, send it straight to output
- If `noOfP > 1` (multiple partitions) — **PURPLE PATH** — we have partial results from multiple workers that need to be combined first

It's like a traffic cop: "You're done? Go left to the exit. You need assembly? Go right to the merge station."

---

### Layer 5: Aggregation
**"The Assembly Station"**

This layer only handles **PURPLE PATH** traffic — partial results that need merging.

**Example:** You created a CountMin with `noOfP=4` (4 parallel workers). Each worker counted frequencies for its portion of the data:
- Worker 0: "user_X appeared 62 times"
- Worker 1: "user_X appeared 63 times"
- Worker 2: "user_X appeared 63 times"
- Worker 3: "user_X appeared 62 times"

The ReduceAggregator **collects all 4 partials** and applies the correct merge strategy:
- **CountMin** — Sum the counts (62+63+63+62 = 250)
- **BloomFilter** — Bitwise OR the bit arrays (combine membership info)
- **AMS** — Sum the sketches
- **HyperLogLog** — Union/merge the registers

The result: one final, complete estimation.

---

### Layer 6: Output
**"The Delivery Truck"**

Takes the final estimation (from either GREEN or PURPLE path), converts it to JSON, and **publishes it to the Kafka output topic**. Downstream consumers (dashboards, APIs, other services) pick it up from there.

---

## The Complete Journey — An Example

Let's trace a real scenario: **"Count unique stock tickers in a Forex stream."**

```
Step 1:  A client sends a Request to Kafka:
         "Create a HyperLogLog (synopsisID=4) on stream 'Forex',
          watching the 'StockID' field, with 4 parallel workers"

Step 2:  Layer 1 parses the JSON into a Request object

Step 3:  Layer 2 (RequestRouter) copies the request 4 times:
         key="Forex_0", key="Forex_1", key="Forex_2", key="Forex_3"
         Layer 2 (DataRouter) registers: "Forex stream now has a HLL listener"

Step 4:  Layer 3 creates 4 HyperLogLog instances (one per partition)

— Now data starts flowing —

Step 5:  A Datapoint arrives: {"dataSetkey":"Forex", "values":{"StockID":"AAPL"}}
         Layer 1 parses it
         Layer 2 routes it to the correct partition
         Layer 3 calls hll.add("AAPL") on that partition's HyperLogLog

— After millions of data points, client wants the count —

Step 6:  Client sends ESTIMATE request (requestID=3)
         Layer 1 parses it
         Layer 2 expands to 4 copies
         Layer 3 calls hll.cardinality() on each partition -> 4 partial estimates

Step 7:  Layer 4 sees noOfP=4 -> PURPLE PATH

Step 8:  Layer 5 collects all 4 partial cardinalities, merges them
         into one final count: "approximately 8,247 unique tickers"

Step 9:  Layer 6 publishes to Kafka:
         {"estimationkey":"Forex_1001", "estimation": 8247, ...}

Step 10: Client reads the result from the output topic
```

---

## Why This Design?

| Design Choice | Why |
|--------------|-----|
| **6 separate layers** | Each has one job — easy to test, debug, and scale independently |
| **Kafka as the glue** | Decouples input producers from the engine; enables hot-swap migration from Flink |
| **GREEN/PURPLE split** | Single-partition results skip merging entirely (faster); multi-partition results get proper aggregation |
| **Checkpointed state** | Synopses survive crashes via HDFS/S3 checkpoints; no more losing hours of accumulated data on restart |
| **SynopsisFactory** | Adding a new algorithm in the future = one new class, no changes to the pipeline |

The entire architecture is designed so that **the outside world (Kafka producers and consumers) never knows whether Flink or Spark is running inside**. Same messages in, same messages out.

---

## Architecture Diagram

See `docs/spark-architecture-diagram.png` for the full color-coded visual diagram showing all 6 layers, data flow paths, and infrastructure components.

For detailed technical specifications, refer to `docs/spark-architecture.md`.
