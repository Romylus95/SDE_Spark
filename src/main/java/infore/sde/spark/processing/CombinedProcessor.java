package infore.sde.spark.processing;

import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import infore.sde.spark.metrics.PipelineMetrics;
import infore.sde.spark.reduceFunctions.ReduceFunction;
import infore.sde.spark.reduceFunctions.SimpleORFunction;
import infore.sde.spark.reduceFunctions.SimpleSumFunction;
import infore.sde.spark.synopses.Synopsis;
import infore.sde.spark.synopses.SynopsisFactory;
import infore.sde.spark.synopses.UnsupportedSynopsisException;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Layers 2+3 merged — routing + synopsis lifecycle in one stateful operator.
 *
 * Keyed by base dataSetKey (e.g. "Forex"). Replaces the two-operator chain:
 *   DataRouter (flatMapGroupsWithState) → shuffle → SynopsisProcessor (flatMapGroupsWithState)
 *
 * What this eliminates vs the old design:
 *   - 1 Spark shuffle between DataRouter and SynopsisProcessor
 *   - SynopsisProcessor's set of HDFS-checkpointed state files
 *   - PURPLE data fan-out (no more "Forex_4_KEYED_i" routing keys)
 *   - PURPLE estimate fan-out + ReduceAggregator merge step (done inline here)
 *
 * PURPLE path: noOfP synopsis instances are stored as "slots" in the same state
 * partition. Incoming data is hash-routed to the correct slot in memory — same
 * hashing logic as before (streamID.hashCode() % noOfP), but without a shuffle.
 * ESTIMATE collects all slot results and reduces them inline.
 */
public class CombinedProcessor
        implements FlatMapGroupsWithStateFunction<String, InputEvent, CombinedState, Estimation> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CombinedProcessor.class);

    private final SDEConfig config;
    private final PipelineMetrics metrics;

    public CombinedProcessor(SDEConfig config) {
        this(config, null);
    }

    public CombinedProcessor(SDEConfig config, PipelineMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
    }

    @Override
    public Iterator<Estimation> call(String key, Iterator<InputEvent> events,
                                     GroupState<CombinedState> state) throws Exception {

        List<Estimation> output = new ArrayList<>();

        if (state.hasTimedOut()) {
            LOG.info("State timeout for key={}, evicting all synopses", key);
            if (metrics != null) metrics.incStateTimeouts();
            if (state.exists()) output.addAll(buildEvictionNotices(key, state.get()));
            state.remove();
            return output.iterator();
        }

        CombinedState cs = state.exists() ? state.get() : new CombinedState();

        while (events.hasNext()) {
            InputEvent event = events.next();
            if (event == null) continue;
            if (event.isRequest() && event.getRequest() != null) {
                handleRequest(event.getRequest(), cs, output, key);
            } else if (event.isData() && event.getDatapoint() != null) {
                handleData(event.getDatapoint(), cs);
            }
        }

        if (cs.isEmpty()) {
            state.remove();
        } else {
            cs.snapshotForSerialization();
            state.update(cs);
            Duration timeout = cs.getTtl() != null ? cs.getTtl() : config.getSafetyNetTtl();
            state.setTimeoutDuration(timeout.toMillis());
        }

        return output.iterator();
    }

    // ── Data handling ─────────────────────────────────────────────────────────

    void handleData(Datapoint dp, CombinedState cs) {
        for (Map.Entry<Integer, Integer> entry : cs.getNoOfPByUid().entrySet()) {
            int uid    = entry.getKey();
            int noOfP  = entry.getValue();
            Map<Integer, Synopsis> slots = cs.getSynopsisSlots().get(uid);
            if (slots == null) continue;

            int slot = noOfP == 1 ? 0 : Math.abs(dp.getStreamID().hashCode()) % noOfP;
            Synopsis synopsis = slots.get(slot);
            if (synopsis == null) continue;
            try {
                synopsis.add(dp.getValues());
            } catch (Exception e) {
                LOG.warn("Error adding data to synopsis uid={}: {}", uid, e.getMessage());
            }
        }
    }

    // ── Request dispatch ──────────────────────────────────────────────────────

    void handleRequest(Request rq, CombinedState cs,
                       List<Estimation> output, String key) {
        int operation = rq.getRequestID() % 10;
        switch (operation) {
            case 1: handleAdd(rq, cs, output, key);      break;
            case 2: handleDelete(rq, cs);                break;
            case 3: handleEstimate(rq, cs, output, key); break;
            default:
                LOG.warn("Unsupported requestID={} for uid={}", rq.getRequestID(), rq.getUid());
        }
    }

    private void handleAdd(Request rq, CombinedState cs,
                           List<Estimation> output, String key) {
        int uid   = rq.getUid();
        int noOfP = rq.getNoOfP();
        if (noOfP < 1) {
            LOG.warn("Invalid noOfP={} for uid={}, skipping", noOfP, uid);
            return;
        }
        try {
            Map<Integer, Synopsis> slots = new HashMap<>();
            for (int slot = 0; slot < noOfP; slot++) {
                slots.put(slot, SynopsisFactory.create(rq.getSynopsisID(), uid, rq.getParam()));
            }
            cs.getSynopsisSlots().put(uid, slots);
            cs.getNoOfPByUid().put(uid, noOfP);
            if (metrics != null) metrics.incSynopsesCreated();
            LOG.info("ADD synopsis uid={} type={} noOfP={} on key={}",
                    uid, rq.getSynopsisID(), noOfP, key);
        } catch (UnsupportedSynopsisException e) {
            LOG.warn("Unsupported synopsisID={} for uid={}", rq.getSynopsisID(), uid);
            output.add(new Estimation(uid, key + "_" + uid, rq.getRequestID(),
                    rq.getSynopsisID(), key, "ERROR: " + e.getMessage(),
                    rq.getParam() != null ? rq.getParam() : new String[0], 1));
        }
    }

    private void handleDelete(Request rq, CombinedState cs) {
        int uid = rq.getUid();
        cs.getSynopsisSlots().remove(uid);
        Integer removed = cs.getNoOfPByUid().remove(uid);
        if (removed != null) {
            if (metrics != null) metrics.incSynopsesDeleted();
            LOG.info("DELETE synopsis uid={}", uid);
        } else {
            LOG.warn("DELETE requested for non-existent synopsis uid={}", uid);
        }
    }

    private void handleEstimate(Request rq, CombinedState cs,
                                List<Estimation> output, String key) {
        int uid = rq.getUid();
        Integer noOfP = cs.getNoOfPByUid().get(uid);
        Map<Integer, Synopsis> slots = cs.getSynopsisSlots().get(uid);

        if (slots == null || noOfP == null) {
            LOG.warn("ESTIMATE for non-existent synopsis uid={}, emitting error notice", uid);
            output.add(new Estimation(uid, key + "_" + uid, -1, 0, key,
                    "ERROR: synopsis uid=" + uid + " does not exist. Re-register before querying.",
                    new String[0], 1));
            return;
        }

        if (noOfP == 1) {
            Synopsis synopsis = slots.get(0);
            if (synopsis == null) return;
            Estimation result = synopsis.estimate(rq);
            if (result != null && result.getEstimation() != null) {
                result.setSynopsisID(synopsis.getAlgorithmType());
                if (metrics != null) metrics.incEstimationsEmitted();
                output.add(result);
            }
        } else {
            // PURPLE: reduce all slots inline — no fan-out or ReduceAggregator needed
            Synopsis first = slots.get(0);
            if (first == null) return;
            int synopsisID = first.getAlgorithmType();

            ReduceFunction reducer = (synopsisID == 2)
                    ? new SimpleORFunction(noOfP, 0, synopsisID, rq.getRequestID())
                    : new SimpleSumFunction(noOfP, 0, synopsisID, rq.getRequestID());

            Estimation template = null;
            for (int slot = 0; slot < noOfP; slot++) {
                Synopsis s = slots.get(slot);
                if (s == null) continue;
                Estimation partial = s.estimate(rq);
                if (partial == null || partial.getEstimation() == null) continue;
                partial.setSynopsisID(synopsisID);
                partial.setNoOfP(noOfP);
                if (template == null) template = new Estimation(partial);
                reducer.add(partial);
            }

            if (template != null) {
                Estimation merged = new Estimation(template);
                merged.setEstimation(reducer.reduce());
                merged.setNoOfP(noOfP);
                if (metrics != null) metrics.incEstimationsEmitted();
                output.add(merged);
            }
        }
    }

    // ── TTL eviction ──────────────────────────────────────────────────────────

    private List<Estimation> buildEvictionNotices(String key, CombinedState cs) {
        List<Estimation> notices = new ArrayList<>();
        for (Map.Entry<Integer, Map<Integer, Synopsis>> entry : cs.getSynopsisSlots().entrySet()) {
            int uid = entry.getKey();
            Map<Integer, Synopsis> slots = entry.getValue();
            Synopsis first = slots.isEmpty() ? null : slots.values().iterator().next();
            int synopsisID = first != null ? first.getAlgorithmType() : 0;
            int noOfP      = cs.getNoOfPByUid().getOrDefault(uid, 1);
            String message = "EVICTED: inactive for TTL period. Re-register synopsis uid=" + uid + " to resume.";
            notices.add(new Estimation(uid, key + "_" + uid, -1, synopsisID, key,
                    message, new String[0], noOfP));
            LOG.info("Eviction notice for uid={} on key={}", uid, key);
        }
        return notices;
    }
}
