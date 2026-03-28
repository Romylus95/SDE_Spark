package infore.sde.spark.processing;

import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import infore.sde.spark.metrics.PipelineMetrics;
import infore.sde.spark.synopses.Synopsis;
import infore.sde.spark.synopses.SynopsisFactory;
import infore.sde.spark.synopses.UnsupportedSynopsisException;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Layer 3 — The heart of the SDE pipeline.
 * Processes both data events and request commands via flatMapGroupsWithState.
 *
 * Handles:
 *   ADD (requestID=1,4)  -> create synopsis via SynopsisFactory, store in state
 *   DATA                 -> iterate active synopses, call add()
 *   ESTIMATE (requestID=3) -> call synopsis.estimate(), emit Estimation
 *   DELETE (requestID=2)   -> remove synopsis from state
 *   TIMEOUT              -> evict stale state, emit eviction notices
 */
public class SynopsisProcessor
        implements FlatMapGroupsWithStateFunction<String, InputEvent, SynopsisProcessorState, Estimation> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SynopsisProcessor.class);

    private final SDEConfig config;
    private final PipelineMetrics metrics;

    public SynopsisProcessor(SDEConfig config) {
        this(config, null);
    }

    public SynopsisProcessor(SDEConfig config, PipelineMetrics metrics) {
        this.config = config;
        this.metrics = metrics;
    }

    @Override
    public Iterator<Estimation> call(String key, Iterator<InputEvent> events,
                                     GroupState<SynopsisProcessorState> state) throws Exception {

        List<Estimation> output = new ArrayList<>();

        // Handle timeout — evict all synopses for this key
        if (state.hasTimedOut()) {
            LOG.info("State timeout for key={}, evicting all synopses", key);
            if (metrics != null) metrics.incStateTimeouts();
            if (state.exists()) {
                output.addAll(buildEvictionNotices(key, state.get()));
            }
            state.remove();
            return output.iterator();
        }

        // Initialize state if needed
        SynopsisProcessorState currentState;
        if (state.exists()) {
            currentState = state.get();
        } else {
            currentState = new SynopsisProcessorState();
        }

        // Collect and sort events: ADD requests first, then DATA, then ESTIMATE/DELETE.
        // Within a micro-batch, event order is not guaranteed. Sorting ensures synopses
        // are created before data is added, and data is added before estimates are run.
        List<InputEvent> addRequests = new ArrayList<>();
        List<InputEvent> dataEvents = new ArrayList<>();
        List<InputEvent> otherRequests = new ArrayList<>();

        while (events.hasNext()) {
            InputEvent event = events.next();
            if (event.isRequest()) {
                int op = event.getRequest().getRequestID() % 10;
                if (op == 1) {
                    addRequests.add(event);
                } else {
                    otherRequests.add(event);
                }
            } else {
                dataEvents.add(event);
            }
        }

        // Process in order: ADDs → DATA → ESTIMATE/DELETE
        for (InputEvent event : addRequests) {
            handleRequest(event.getRequest(), currentState, output);
        }
        for (InputEvent event : dataEvents) {
            handleData(event.getDatapoint(), currentState);
        }
        for (InputEvent event : otherRequests) {
            handleRequest(event.getRequest(), currentState, output);
        }

        // Update state and reset timeout
        if (currentState.isEmpty()) {
            state.remove();
        } else {
            currentState.snapshotForSerialization();
            state.update(currentState);
            Duration timeout = currentState.getTtl() != null
                    ? currentState.getTtl()
                    : config.getSafetyNetTtl();
            state.setTimeoutDuration(timeout.toMillis());
        }

        return output.iterator();
    }

    private void handleData(Datapoint datapoint, SynopsisProcessorState currentState) {
        for (Synopsis synopsis : currentState.getSynopses().values()) {
            try {
                synopsis.add(datapoint.getValues());
            } catch (Exception e) {
                LOG.warn("Error adding data to synopsis uid={}: {}",
                        synopsis.getSynopsisID(), e.getMessage());
            }
        }
    }

    private void handleRequest(Request rq, SynopsisProcessorState currentState,
                               List<Estimation> output) {
        int operation = rq.getRequestID() % 10;

        switch (operation) {
            case 1 -> handleAdd(rq, currentState, output);
            case 2 -> handleDelete(rq, currentState);
            case 3 -> handleEstimate(rq, currentState, output);
            default -> LOG.warn("Unsupported requestID={} for uid={}", rq.getRequestID(), rq.getUid());
        }
    }

    private void handleAdd(Request rq, SynopsisProcessorState currentState,
                           List<Estimation> output) {
        try {
            Synopsis synopsis = SynopsisFactory.create(rq.getSynopsisID(), rq.getUid(), rq.getParam());
            currentState.addSynopsis(rq.getUid(), synopsis);
            if (metrics != null) metrics.incSynopsesCreated();
            LOG.info("ADD synopsis uid={} type={} on key={}",
                    rq.getUid(), rq.getSynopsisID(), rq.getDataSetKey());
        } catch (UnsupportedSynopsisException e) {
            LOG.warn("Unsupported synopsisID={} for uid={}", rq.getSynopsisID(), rq.getUid());
            Estimation error = new Estimation(
                    rq.getUid(),
                    rq.getDataSetKey() + "_" + rq.getUid(),
                    rq.getRequestID(),
                    rq.getSynopsisID(),
                    rq.getDataSetKey(),
                    "ERROR: " + e.getMessage(),
                    rq.getParam() != null ? rq.getParam() : new String[0],
                    1
            );
            output.add(error);
        }
    }

    private void handleDelete(Request rq, SynopsisProcessorState currentState) {
        Synopsis removed = currentState.removeSynopsis(rq.getUid());
        if (removed != null) {
            if (metrics != null) metrics.incSynopsesDeleted();
            LOG.info("DELETE synopsis uid={} on key={}", rq.getUid(), rq.getDataSetKey());
        } else {
            LOG.warn("DELETE requested for non-existent synopsis uid={}", rq.getUid());
        }
    }

    private void handleEstimate(Request rq, SynopsisProcessorState currentState,
                                List<Estimation> output) {
        Synopsis synopsis = currentState.getSynopsis(rq.getUid());
        if (synopsis == null) {
            LOG.warn("ESTIMATE requested for non-existent synopsis uid={}", rq.getUid());
            return;
        }

        Estimation estimation = synopsis.estimate(rq);
        if (estimation != null && estimation.getEstimation() != null) {
            if (metrics != null) metrics.incEstimationsEmitted();
            output.add(estimation);
        }
    }

    private List<Estimation> buildEvictionNotices(String key, SynopsisProcessorState currentState) {
        List<Estimation> notices = new ArrayList<>();
        for (var entry : currentState.getSynopses().entrySet()) {
            int uid = entry.getKey();
            Synopsis synopsis = entry.getValue();
            String message = "EVICTED: inactive for TTL period. Re-register synopsis uid=" + uid + " to resume.";

            Estimation notice = new Estimation(
                    uid,
                    key + "_" + uid,
                    -1,
                    synopsis.getSynopsisID(),
                    key,
                    message,
                    new String[0],
                    1
            );
            notices.add(notice);
            LOG.info("Eviction notice for uid={} on key={}", uid, key);
        }
        return notices;
    }
}
