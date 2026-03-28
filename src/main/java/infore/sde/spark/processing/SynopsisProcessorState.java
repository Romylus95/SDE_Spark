package infore.sde.spark.processing;

import infore.sde.spark.synopses.Synopsis;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * State held per grouping key in the SynopsisProcessor.
 * Stored in-memory via GroupState, checkpointed to HDFS/S3.
 */
public class SynopsisProcessorState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<Integer, Synopsis> synopses = new HashMap<>();
    private Duration ttl;

    public Map<Integer, Synopsis> getSynopses() { return synopses; }

    public Duration getTtl() { return ttl; }
    public void setTtl(Duration ttl) { this.ttl = ttl; }

    public void addSynopsis(int uid, Synopsis synopsis) {
        synopses.put(uid, synopsis);
    }

    public Synopsis getSynopsis(int uid) {
        return synopses.get(uid);
    }

    public Synopsis removeSynopsis(int uid) {
        return synopses.remove(uid);
    }

    public boolean isEmpty() {
        return synopses.isEmpty();
    }

    /**
     * Snapshot all synopsis transient state before serialization.
     * Must be called before state.update() to ensure Kryo captures
     * non-serializable third-party objects as byte arrays.
     */
    public void snapshotForSerialization() {
        for (Synopsis synopsis : synopses.values()) {
            synopsis.snapshotState();
        }
    }
}
