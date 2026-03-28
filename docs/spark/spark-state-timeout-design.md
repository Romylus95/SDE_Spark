# SDE Spark Edition — State Timeout Design

## Document Information

| Field | Value |
|-------|-------|
| **Document Type** | Feature Design — State Lifecycle Management |
| **Component** | `SynopsisProcessor` (Layer 3) |
| **Status** | Approved for Implementation |
| **Related Docs** | `docs/spark-architecture.md`, `docs/prd.md` |

---

## 1. Problem Statement

Without a cleanup mechanism, in-memory state grows unboundedly. Clients may:
- Crash before sending a DELETE request (`requestID=2`)
- Abandon synopses after a short experiment
- Forget to clean up after a one-time query

At 10,000 concurrent synopsis instances (NFR9), orphaned state accumulates silently and permanently in the Flink version. The Spark migration must solve this.

**Constraint:** Synopses are expected to be **long-lived** (e.g., a BloomFilter tracking fraud card numbers for months, a HyperLogLog counting daily uniques indefinitely). A fixed TTL that evicts based on age alone would silently destroy active, valuable state — unacceptable.

---

## 2. Design Decision

### Chosen Approach: Activity-Based TTL (Opt-In per Synopsis)

| Approach | Decision | Reason |
|----------|----------|--------|
| No timeout | Rejected | Unbounded state growth on abandoned synopses |
| Fixed TTL (age-based) | Rejected | Kills long-lived synopses that are still actively used |
| Activity-based TTL (reset on use) | **Accepted** | Only evicts truly abandoned synopses |
| Global safety-net (very long TTL) | **Accepted** | Last-resort for synopses with no explicit TTL |

### Rules

1. **Opt-in per synopsis** — TTL is specified at registration time via the `ttl` field in the ADD request. If absent, no explicit timeout applies.
2. **Activity-based** — the timeout timer resets on every event (data arrival or any request). Only a synopsis with zero activity for the full TTL duration gets evicted.
3. **Global safety-net** — all synopses, regardless of explicit TTL, are subject to a configurable global maximum inactivity period (default: **7 days**). This is the last line of defence against permanently orphaned state.
4. **Never silent** — eviction always emits a notification `Estimation` to the Kafka output topic before removing state. Clients can detect and re-register.

---

## 3. API Change — TTL Field in ADD Request

The `ttl` field is an **optional addition** to the existing ADD request format. Existing clients that omit it are unaffected (backward compatible).

### Request Format (with TTL)

```json
{
  "dataSetkey": "Forex",
  "requestID": 1,
  "synopsisID": 1,
  "uid": 42,
  "streamID": "EURUSD",
  "param": ["StockID", "price", "Queryable", "0.01", "4"],
  "noOfP": 2,
  "ttl": "24h"
}
```

### TTL Format

| Value | Meaning |
|-------|---------|
| `"1h"` | 1 hour of inactivity before eviction |
| `"24h"` | 24 hours of inactivity |
| `"7d"` | 7 days of inactivity |
| absent / `null` | No explicit TTL — global safety-net applies (7 days default) |

Supported units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).

---

## 4. Eviction Notification

Before removing state, the processor **must** emit an `Estimation` to the output Kafka topic. This allows clients to detect the eviction and re-register if the synopsis is still needed.

### Eviction Notification Format

```json
{
  "key": "Forex_42",
  "estimationkey": "Forex_42",
  "streamID": "EURUSD",
  "uid": 42,
  "requestID": -1,
  "synopsisID": 1,
  "estimation": "EVICTED: inactive for 24h. Re-register synopsis uid=42 to resume.",
  "param": ["StockID", "price", "Queryable", "0.01", "4"],
  "noOfP": 2
}
```

**Conventions:**
- `requestID = -1` signals a system-generated event (not a client query response)
- `estimation` field contains a human-readable eviction reason string
- All original registration metadata (`uid`, `synopsisID`, `param`, `noOfP`) is echoed back so the client has everything needed to re-register

---

## 5. Implementation

### 5.1 State Model Change

Add `ttl` and `lastActivityMs` to `SynopsisProcessorState`:

```java
public class SynopsisProcessorState implements Serializable {
    private Map<Integer, Synopsis> synopses;           // uid → synopsis instance
    private Map<Integer, SynopsisMetadata> metadata;   // uid → registration metadata
    private Duration ttl;          // null = no explicit TTL (safety-net applies)
    private long lastActivityMs;   // epoch ms of last data or request event
}

public class SynopsisMetadata implements Serializable {
    private int synopsisId;
    private String streamId;
    private String[] param;
    private int noOfP;
}
```

### 5.2 Spark Timeout Configuration

Use `ProcessingTimeTimeout` — wall-clock based, does not depend on event watermarks:

```java
dataset.groupByKey(e -> e.getDataSetKey())
       .flatMapGroupsWithState(
           stateFunction,
           Encoders.kryo(SynopsisProcessorState.class),
           Encoders.kryo(Estimation.class),
           OutputMode.Append(),
           GroupStateTimeout.ProcessingTimeTimeout()
       );
```

### 5.3 SynopsisProcessor Logic

```java
Iterator<Estimation> call(
        String key,
        Iterator<SynopsisInput> events,
        GroupState<SynopsisProcessorState> state) {

    // ── Eviction path ────────────────────────────────────────────────────
    if (state.hasTimedOut()) {
        List<Estimation> notices = buildEvictionNotices(key, state.get());
        log.info("Evicting state for key={} — inactivity timeout", key);
        state.remove();
        return notices.iterator();
    }

    // ── Normal processing path ───────────────────────────────────────────
    List<Estimation> output = new ArrayList<>();

    while (events.hasNext()) {
        SynopsisInput event = events.next();

        if (event instanceof RequestEvent re) {
            handleRequest(re.request(), state, output);
        } else if (event instanceof DataEvent de) {
            handleData(de.datapoint(), state, output);
        }
    }

    // ── Reset activity timer after every batch ───────────────────────────
    resetActivityTimer(state);

    return output.iterator();
}

private void resetActivityTimer(GroupState<SynopsisProcessorState> state) {
    SynopsisProcessorState s = state.get();
    s.setLastActivityMs(System.currentTimeMillis());
    state.update(s);

    // Per-synopsis TTL takes precedence; fall back to global safety-net
    Duration timeout = (s.getTtl() != null)
        ? s.getTtl()
        : globalConfig.getSafetyNetTtl();   // default: Duration.ofDays(7)

    state.setTimeoutDuration(timeout.toMillis() + " milliseconds");
}

private List<Estimation> buildEvictionNotices(
        String key, SynopsisProcessorState state) {

    List<Estimation> notices = new ArrayList<>();
    for (Map.Entry<Integer, SynopsisMetadata> e : state.getMetadata().entrySet()) {
        SynopsisMetadata meta = e.getValue();
        notices.add(Estimation.evictionNotice(key, meta));
    }
    return notices;
}
```

### 5.4 TTL Parsing Helper

```java
public class TtlParser {
    // Parses "24h", "7d", "30m", "3600s" → Duration
    public static Duration parse(String ttl) {
        if (ttl == null || ttl.isBlank()) return null;
        char unit = ttl.charAt(ttl.length() - 1);
        long value = Long.parseLong(ttl.substring(0, ttl.length() - 1));
        return switch (unit) {
            case 's' -> Duration.ofSeconds(value);
            case 'm' -> Duration.ofMinutes(value);
            case 'h' -> Duration.ofHours(value);
            case 'd' -> Duration.ofDays(value);
            default  -> throw new IllegalArgumentException(
                "Invalid TTL format: " + ttl + ". Use s/m/h/d suffix.");
        };
    }
}
```

---

## 6. Lifecycle Flow

### Synopsis with explicit TTL (`ttl="24h"`)

```
ADD request (ttl="24h")
    │
    ├── state created, timer set to 24h
    │
    ├── data arrives       → timer resets to 24h from now
    ├── data arrives       → timer resets to 24h from now
    ├── ESTIMATE request   → timer resets to 24h from now
    │
    │   [ 24 hours pass with zero activity ]
    │
    └── TIMEOUT fires
            → emit eviction notice to Kafka
            → state.remove()
```

### Synopsis with no TTL (safety-net only)

```
ADD request (no ttl field)
    │
    ├── state created, timer set to 7 days (global safety-net)
    │
    ├── active for months → timer keeps resetting on every event
    │
    │   [ only if 7 days pass with ZERO activity ]
    │
    └── TIMEOUT fires
            → emit eviction notice to Kafka
            → state.remove()
```

### Explicit DELETE (unchanged behaviour)

```
DELETE request (requestID=2)
    │
    └── immediate state.remove() — no eviction notice emitted
        (client-initiated deletion is intentional, no notification needed)
```

---

## 7. Configuration

All timeout values are externally configurable via `SDEConfig`:

```java
public class SDEConfig {
    // Global safety-net TTL for synopses with no explicit ttl field
    // Default: 7 days
    private Duration safetyNetTtl = Duration.ofDays(7);

    // Minimum allowed TTL (prevent clients setting 1-second TTLs by mistake)
    // Default: 1 hour
    private Duration minimumTtl = Duration.ofHours(1);
}
```

Configurable via `spark-submit` arguments or application properties file.

---

## 8. Backward Compatibility

| Scenario | Behaviour |
|----------|-----------|
| Existing client sends ADD request without `ttl` field | No change — safety-net TTL applies silently |
| Existing client sends DELETE request | No change — immediate removal as before |
| Client receives eviction notice (`requestID=-1`) | New message type — clients should handle gracefully (log, alert, re-register) |

The `ttl` field is purely additive. No existing request or response fields are changed or removed.

---

## 9. Testing Requirements

| Test | Description |
|------|-------------|
| Unit: TtlParser | Parse all formats (s/m/h/d), reject invalid formats |
| Unit: resetActivityTimer | Verify timer resets after data event, request event |
| Unit: eviction notice | Verify Estimation fields: `requestID=-1`, correct metadata echoed |
| Unit: no-TTL fallback | Verify safety-net duration applied when `ttl` is absent |
| Integration: TTL eviction | Set short TTL (e.g. 5s in test), send no activity, verify eviction notice on Kafka |
| Integration: no false eviction | Active synopsis never evicted within TTL window |
| Integration: DELETE vs timeout | Explicit DELETE does not emit eviction notice |
| Integration: re-register after eviction | Client receives notice, re-registers, data flows correctly |

---

## 10. Open Questions

| # | Question | Impact |
|---|----------|--------|
| 1 | Should eviction notice be emitted per synopsis (per UID) or once per state key? | For keys with many synopses, per-UID is more useful to clients but produces more messages. **Recommendation: per UID.** |
| 2 | Should minimum TTL (`1h`) be enforced server-side or just documented? | Prevents accidental sub-minute TTLs from causing thrashing. **Recommendation: enforce server-side, return error Estimation if violated.** |
| 3 | Should the safety-net TTL be configurable per deployment or fixed? | Some research environments may want longer (e.g. 30 days). **Recommendation: configurable via SDEConfig.** |
