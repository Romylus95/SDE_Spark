package infore.sde.spark.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;

import java.io.Serializable;

/**
 * Abstract base class for all synopsis (probabilistic data structure) implementations.
 *
 * Each synopsis instance is identified by a unique uid (synopsisID field) and configured
 * with a keyIndex (which JSON field to use as the key), valueIndex (which field to aggregate),
 * and operationMode (e.g., "Queryable").
 *
 * Subclasses must implement:
 *   - add(values): ingest a data point from the stream
 *   - estimate(key/request): query the synopsis for an estimation
 *   - merge(other): combine with another synopsis (for PURPLE path aggregation)
 *   - snapshotState(): serialize transient internal state for checkpointing (if needed)
 *
 * Supported implementations: CountMin (1), Bloomfilter (2), AMSsynopsis (3), HyperLogLogSynopsis (4)
 */
public abstract class Synopsis implements Serializable {

    private static final long serialVersionUID = 1L;

    protected int synopsisID;    // unique instance identifier (uid from the ADD request)
    protected String keyIndex;   // JSON field name used as the key (e.g., "StockID")
    protected String valueIndex; // JSON field name used as the value (e.g., "price")
    protected String operationMode; // operation mode (e.g., "Queryable")

    protected Synopsis(int uid, String keyIndex, String valueIndex) {
        this.synopsisID = uid;
        this.keyIndex = keyIndex;
        this.valueIndex = valueIndex;
    }

    protected Synopsis(int uid, String keyIndex, String valueIndex, String operationMode) {
        this(uid, keyIndex, valueIndex);
        this.operationMode = operationMode;
    }

    public abstract void add(JsonNode values);

    public abstract Object estimate(Object key);

    public abstract Estimation estimate(Request rq);

    public abstract Synopsis merge(Synopsis other);

    /**
     * Snapshot internal transient state into serializable byte arrays.
     * Called before Kryo/Java serialization to ensure non-serializable
     * third-party objects are captured. Default is no-op (for synopses
     * that are fully serializable like CountMin).
     */
    public void snapshotState() {
        // Override in subclasses with transient fields
    }

    public int getSynopsisID() { return synopsisID; }

    public String getKeyIndex() { return keyIndex; }

    public String getValueIndex() { return valueIndex; }

    public String getOperationMode() { return operationMode; }
}
