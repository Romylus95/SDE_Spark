package infore.sde.spark.routing;

import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Request;
import infore.sde.spark.processing.InputEvent;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Layer 2 — Unified request + data routing with stateful fan-out.
 *
 * Receives the union of request and data streams (tagged as InputEvent).
 * Manages routing state per dataset key using flatMapGroupsWithState.
 *
 * For requests with noOfP > 1 (PURPLE path):
 *   - Registers parallelism level in routing state (so future data gets fanned out)
 *   - Fans out the request itself to N keyed partition keys: "{key}_{noOfP}_KEYED_{i}"
 * For requests with noOfP == 1 (GREEN path):
 *   - Forwards request as-is (no fan-out needed)
 * For data events:
 *   - Routes to all registered keyed partition keys (hash-based slot assignment)
 *   - Also forwards with original key for noOfP=1 synopses
 *
 * Events within each micro-batch are processed in arrival order. If an ADD and DATA
 * event arrive in the same batch, the data event may be processed before the registration
 * exists — that batch's data is missed for the new synopsis. This is an accepted
 * one-batch imprecision at registration time.
 *
 * Supports processing-time timeout to evict stale routing state for inactive keys.
 */
public class DataRouter
        implements FlatMapGroupsWithStateFunction<String, InputEvent, RoutingState, InputEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataRouter.class);

    private final SDEConfig config;

    public DataRouter(SDEConfig config) {
        this.config = config;
    }

    @Override
    public Iterator<InputEvent> call(String key, Iterator<InputEvent> events,
                                     GroupState<RoutingState> state) throws Exception {

        List<InputEvent> output = new ArrayList<>();

        // Handle timeout — evict stale routing state for keys with no active synopses
        if (state.hasTimedOut()) {
            LOG.info("Routing state timeout for key={}, evicting", key);
            state.remove();
            return output.iterator();
        }

        RoutingState routingState;
        if (state.exists()) {
            routingState = state.get();
        } else {
            routingState = new RoutingState();
        }

        while (events.hasNext()) {
            InputEvent event = events.next();
            if (event == null) continue;
            if (event.isRequest() && event.getRequest() != null) {
                handleRequest(event.getRequest(), routingState, output);
            } else if (event.isData() && event.getDatapoint() != null) {
                handleData(event.getDatapoint(), routingState, output);
            }
        }

        if (routingState.getRegistrations().isEmpty()) {
            state.remove();
        } else {
            state.update(routingState);
            state.setTimeoutDuration(config.getRoutingStateTtl().toMillis());
        }
        return output.iterator();
    }

    private void handleRequest(Request rq, RoutingState routingState,
                               List<InputEvent> output) {
        int operation = rq.getRequestID() % 10;

        if (operation == 1) {
            // ADD: register parallelism and fan out request
            if (rq.getNoOfP() < 1) {
                LOG.warn("DataRouter: invalid noOfP={} for uid={}, skipping", rq.getNoOfP(), rq.getUid());
                return;
            }
            if (rq.getNoOfP() > 1) {
                LOG.info("DataRouter: registered parallelism={} for uid={} on key={}",
                        rq.getNoOfP(), rq.getUid(), rq.getDataSetKey());

                // Fan out to keyed partition keys; streamID is not used for ADD
                String baseKey = rq.getDataSetKey();
                for (int i = 0; i < rq.getNoOfP(); i++) {
                    Request copy = new Request(
                            baseKey + "_" + rq.getNoOfP() + "_KEYED_" + i,
                            rq.getRequestID(), rq.getSynopsisID(), rq.getUid(),
                            null, rq.getParam(), rq.getNoOfP());
                    output.add(InputEvent.request(copy));
                }
            } else {
                // noOfP=1: forward as copy without streamID (unused for ADD)
                Request copy = new Request(
                        rq.getDataSetKey(), rq.getRequestID(), rq.getSynopsisID(), rq.getUid(),
                        null, rq.getParam(), rq.getNoOfP());
                output.add(InputEvent.request(copy));
            }
            routingState.getRegistrations().put(rq.getUid(),
                    new RoutingState.RoutingRegistration(rq.getRequestID(), rq.getNoOfP(), rq.getDataSetKey()));

        } else if (operation == 2) {
            // DELETE: unregister and fan out; only dataSetkey, requestID, uid are used
            RoutingState.RoutingRegistration reg = routingState.getRegistrations().remove(rq.getUid());
            if (reg != null && reg.getNoOfP() > 1) {
                String baseKey = reg.getDataSetKey();
                for (int i = 0; i < reg.getNoOfP(); i++) {
                    Request copy = new Request(
                            baseKey + "_" + reg.getNoOfP() + "_KEYED_" + i,
                            rq.getRequestID(), 0, rq.getUid(),
                            null, null, reg.getNoOfP());
                    output.add(InputEvent.request(copy));
                }
            } else {
                Request copy = new Request(
                        rq.getDataSetKey(), rq.getRequestID(), 0, rq.getUid(),
                        null, null, 1);
                output.add(InputEvent.request(copy));
            }

        } else {
            // ESTIMATE — derive noOfP from stored registration; strip synopsisID and streamID
            // (synopsisID is restored by SynopsisProcessor from the stored synopsis object)
            RoutingState.RoutingRegistration reg = routingState.getRegistrations().get(rq.getUid());
            if (reg == null) {
                LOG.warn("DataRouter: ESTIMATE for unknown uid={}, no registration found, forwarding as-is", rq.getUid());
                output.add(InputEvent.request(rq));
            } else if (reg.getNoOfP() > 1) {
                String baseKey = rq.getDataSetKey();
                int fanOut = reg.getNoOfP();
                for (int i = 0; i < fanOut; i++) {
                    Request copy = new Request(
                            baseKey + "_" + fanOut + "_KEYED_" + i,
                            rq.getRequestID(), 0, rq.getUid(),
                            null, rq.getParam(), fanOut);
                    output.add(InputEvent.request(copy));
                }
            } else {
                Request copy = new Request(
                        rq.getDataSetKey(), rq.getRequestID(), 0, rq.getUid(),
                        null, rq.getParam(), reg.getNoOfP());
                output.add(InputEvent.request(copy));
            }
        }
    }

    private void handleData(Datapoint datapoint, RoutingState routingState,
                            List<InputEvent> output) {
        Set<Integer> activeLevels = routingState.getRegistrations().values().stream()
                .map(RoutingState.RoutingRegistration::getNoOfP)
                .filter(p -> p > 1)
                .collect(Collectors.toSet());

        if (activeLevels.isEmpty()) {
            output.add(InputEvent.data(datapoint));
            return;
        }

        for (int parallelism : activeLevels) {
            String baseKey = datapoint.getDataSetKey();
            // Hash the stream data to pick the correct partition
            int slot = Math.abs(datapoint.getStreamID().hashCode()) % parallelism;
            String routedKey = baseKey + "_" + parallelism + "_KEYED_" + slot;

            LOG.info("DataRouter: routing data streamID={} -> {} (slot={})",
                    datapoint.getStreamID(), routedKey, slot);
            Datapoint routed = new Datapoint(routedKey, datapoint.getStreamID(), datapoint.getValues());
            output.add(InputEvent.data(routed));
        }

        // Also forward with original key for noOfP=1 synopses
        output.add(InputEvent.data(datapoint));
    }
}
