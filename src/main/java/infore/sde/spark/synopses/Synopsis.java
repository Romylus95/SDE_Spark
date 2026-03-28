package infore.sde.spark.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;

import java.io.Serializable;

public abstract class Synopsis implements Serializable {

    private static final long serialVersionUID = 1L;

    protected int synopsisID;
    protected String keyIndex;
    protected String valueIndex;
    protected String operationMode;

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
