# End-to-End Validated Flow — GREEN & PURPLE Paths

> Validated 2026-03-25 using `EndToEndTestApp` + `EndToEndTestProducer`.
> Both results confirmed on Kafka `estimation_topic`.

---

## GREEN Path (noOfP=1) — CountMin Sketch

### Step 1: ADD CountMin (uid=10)

Producer sends to `request_topic`:
```json
{"dataSetkey":"Forex", "requestID":1, "synopsisID":1, "uid":10,
 "param":["StockID","price","Queryable","0.01","0.99","42"], "noOfP":1}
```

Flow:
1. **Layer 1** (KafkaIngestionLayer) reads from `request_topic`, parses JSON into `Request` object
2. **Layer 2** (DataRouter) sees `noOfP=1` — no parallelism to register, forwards request as-is with key=`Forex`
3. **Layer 3** (SynopsisProcessor) grouped by key=`Forex`, sees `requestID=1` (ADD), creates a CountMin sketch with uid=10. Params: keyField=`StockID`, valueField=`price`, epsilon=0.01, confidence=0.99, seed=42

### Step 2: Send 5 data events

Producer sends to `data_topic`:
```
AAPL  182.50  (streamID=EURUSD)
AAPL  183.00  (streamID=EURUSD)
GOOG  141.20  (streamID=EURUSD)
AAPL  184.00  (streamID=EURUSD)
MSFT  378.90  (streamID=EURUSD)
```

Flow:
1. **Layer 1** parses each into `Datapoint` with `dataSetKey=Forex`
2. **Layer 2** (DataRouter) — no parallelism registered for key=`Forex` (uid=10 has noOfP=1), so data forwarded as-is with key=`Forex`
3. **Layer 3** (SynopsisProcessor) grouped by key=`Forex`, finds uid=10's CountMin sketch, calls `add()` for each datapoint. CountMin hashes `StockID` and adds `price` as the value:
   - `add("AAPL", 182.50)` — internal counters updated
   - `add("AAPL", 183.00)` — counters updated
   - `add("GOOG", 141.20)` — counters updated
   - `add("AAPL", 184.00)` — counters updated
   - `add("MSFT", 378.90)` — counters updated

### Step 3: ESTIMATE

Producer sends:
```json
{"dataSetkey":"Forex", "requestID":3, "synopsisID":1, "uid":10,
 "param":["AAPL"], "noOfP":1}
```

Flow:
1. **Layer 1** parses into `Request`
2. **Layer 2** (DataRouter) forwards as-is (noOfP=1)
3. **Layer 3** (SynopsisProcessor) sees `requestID=3` (ESTIMATE), finds uid=10's CountMin, calls `estimate("AAPL")` — returns **549.0** (182.50 + 183.00 + 184.00, the sum of all values added under key "AAPL"). Emits an `Estimation` object.
4. **Layer 4** (PathSplitter) checks `noOfP==1` — **GREEN path** — goes directly to output
5. *(Layer 5 skipped — no aggregation needed)*
6. **Layer 6** (KafkaOutputLayer) serializes to JSON, writes to `estimation_topic`:
```json
{"key":"Forex","estimationkey":"10","uid":10,"requestID":3,
 "synopsisID":1,"estimation":"549.0","param":["AAPL"],"noOfP":1}
```

---

## PURPLE Path (noOfP=2) — CountMin Sketch with Parallel Partitions

### Step 4: ADD CountMin (uid=50, noOfP=2)

Producer sends to `request_topic`:
```json
{"dataSetkey":"Forex", "requestID":1, "synopsisID":1, "uid":50,
 "param":["StockID","price","Queryable","0.01","0.99","42"], "noOfP":2}
```

Flow:
1. **Layer 1** parses into `Request` with key=`Forex`, noOfP=2
2. **Layer 2** (DataRouter) grouped by key=`Forex`:
   - **Registers** `parallelism=2` in routing state — DataRouter now knows future data on `Forex` must be fanned out to 2 keyed partitions
   - **Fans out** the ADD request into 2 copies:
     - Copy 1: key=`Forex_2_KEYED_0`, uid=50, same params
     - Copy 2: key=`Forex_2_KEYED_1`, uid=50, same params
3. **Layer 3** (SynopsisProcessor) — TWO groups receive the ADD:
   - Group `Forex_2_KEYED_0`: creates CountMin sketch uid=50
   - Group `Forex_2_KEYED_1`: creates CountMin sketch uid=50

   Two independent copies of the same synopsis, one per partition.

### Step 5: Send 6 data events with varied streamIDs

Producer sends to `data_topic`:
```
streamID=EURUSD  StockID=AAPL  price=185.00
streamID=GBPUSD  StockID=AAPL  price=186.00
streamID=USDJPY  StockID=AAPL  price=187.00
streamID=EURUSD  StockID=TSLA  price=248.50
streamID=GBPUSD  StockID=NVDA  price=920.00
streamID=USDJPY  StockID=GOOG  price=142.00
```

Flow:
1. **Layer 1** parses each into `Datapoint` with `dataSetKey=Forex`
2. **Layer 2** (DataRouter) grouped by key=`Forex`:
   - Routing state has `parallelism=2` registered — each datapoint is hashed:
     - `slot = abs(streamID.hashCode()) % 2`
     - `EURUSD` hashes to slot **0** — routed to `Forex_2_KEYED_0`
     - `GBPUSD` hashes to slot **1** — routed to `Forex_2_KEYED_1`
     - `USDJPY` hashes to slot **1** — routed to `Forex_2_KEYED_1`
   - Each datapoint also forwarded with original key `Forex` (for noOfP=1 synopses like uid=10)

   Data distribution to keyed partitions:

   | Partition | Data received |
   |-----------|--------------|
   | `Forex_2_KEYED_0` | AAPL 185.00, TSLA 248.50 |
   | `Forex_2_KEYED_1` | AAPL 186.00, AAPL 187.00, NVDA 920.00, GOOG 142.00 |

3. **Layer 3** (SynopsisProcessor):
   - Group `Forex_2_KEYED_0`: uid=50's CountMin adds AAPL=185.00 and TSLA=248.50
   - Group `Forex_2_KEYED_1`: uid=50's CountMin adds AAPL=186.00, AAPL=187.00, NVDA=920.00, GOOG=142.00

### Step 6: ESTIMATE (noOfP=2)

Producer sends:
```json
{"dataSetkey":"Forex", "requestID":3, "synopsisID":1, "uid":50,
 "param":["AAPL"], "noOfP":2}
```

Flow:
1. **Layer 1** parses into `Request`
2. **Layer 2** (DataRouter) fans out the ESTIMATE to both partitions:
   - Copy 1: key=`Forex_2_KEYED_0`, query "AAPL"
   - Copy 2: key=`Forex_2_KEYED_1`, query "AAPL"
3. **Layer 3** (SynopsisProcessor) — each partition estimates independently:
   - `Forex_2_KEYED_0`: estimate("AAPL") = **185.0** (only one AAPL value in this partition)
   - `Forex_2_KEYED_1`: estimate("AAPL") = **373.0** (186.00 + 187.00)
   - Each emits a partial `Estimation` with `noOfP=2`
4. **Layer 4** (PathSplitter) checks `noOfP > 1` — **PURPLE path** — routes to ReduceAggregator
5. **Layer 5** (ReduceAggregator) grouped by `uid=50`:
   - Receives partial 1: estimation=185.0
   - Receives partial 2: estimation=373.0
   - `SimpleSumFunction.add()` counts: 2 of 2 received — `reduce()` sums them: 185.0 + 373.0 = **558.0**
   - Emits merged `Estimation`
6. **Layer 6** writes to `estimation_topic`:
```json
{"key":"Forex_2_KEYED_0","estimationkey":"50","uid":50,"requestID":3,
 "synopsisID":1,"estimation":558.0,"param":["AAPL"],"noOfP":2}
```

---

## Summary of Numbers

| Path | uid | Query | Partition data | Partial results | Final |
|------|-----|-------|---------------|----------------|-------|
| GREEN | 10 | AAPL | 182.50 + 183.00 + 184.00 | — | **549.0** |
| PURPLE slot 0 | 50 | AAPL | 185.00 | 185.0 | — |
| PURPLE slot 1 | 50 | AAPL | 186.00 + 187.00 | 373.0 | — |
| PURPLE merged | 50 | AAPL | — | 185.0 + 373.0 | **558.0** |

---

## Key Architecture Note

DataRouter handles both request fan-out AND data fan-out in a single stateful operator. This is critical because request registration and data routing must share the same partition state — if they were separate operators, the ADD request (which registers parallelism) would be in a different group-by partition than the data events that need to be routed.
