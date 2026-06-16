package infore.sde.spark.processing;

import infore.sde.spark.synopses.Synopsis;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Combined state for CombinedProcessor — holds routing registrations and all synopsis
 * slots for a given base dataSetKey.
 *
 * PURPLE path: synopsisSlots[uid] contains noOfP Synopsis instances (slot 0..noOfP-1).
 * GREEN path:  synopsisSlots[uid] contains one instance (slot 0).
 *
 * Replaces the separate RoutingState (DataRouter) + SynopsisProcessorState (SynopsisProcessor)
 * pair, cutting the number of checkpointed state operators from 2 to 1.
 */
public class CombinedState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** uid -> noOfP (routing + slot-count for data dispatch and ESTIMATE reduction) */
    private final Map<Integer, Integer> noOfPByUid = new HashMap<>();

    /** uid -> (slot -> Synopsis) */
    private final Map<Integer, Map<Integer, Synopsis>> synopsisSlots = new HashMap<>();

    private Duration ttl;

    public Map<Integer, Integer> getNoOfPByUid() { return noOfPByUid; }
    public Map<Integer, Map<Integer, Synopsis>> getSynopsisSlots() { return synopsisSlots; }

    public Duration getTtl() { return ttl; }
    public void setTtl(Duration ttl) { this.ttl = ttl; }

    public boolean isEmpty() { return synopsisSlots.isEmpty(); }

    public void snapshotForSerialization() {
        for (Map<Integer, Synopsis> slots : synopsisSlots.values()) {
            for (Synopsis s : slots.values()) {
                s.snapshotState();
            }
        }
    }
}
