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

/**
 * Layer 2 — Combined request + data routing with state.
 *
 * Receives the ORIGINAL (un-routed) request stream unioned with data.
 * For requests with noOfP > 1:
 *   - Registers parallelism in routing state (so future data gets fanned out)
 *   - Fans out the request itself to keyed partition keys (like RequestRouter)
 * For data:
 *   - Routes to all registered keyed partition keys
 *   - Also forwards with original key for noOfP=1 synopses
 *
 * This replaces the separate RequestRouter + DataRouter architecture.
 * The key insight: request fan-out and data fan-out must share the same
 * groupByKey partition so the routing state is visible to both.
 *
 * Replaces: dataRouterCoFlatMap.java from Flink
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

        // Sort events: ADD/DELETE requests first, then data, then other requests.
        // This ensures routing registrations are applied before data arrives
        // (micro-batch event order is arbitrary).
        List<InputEvent> addDeleteRequests = new ArrayList<>();
        List<InputEvent> dataEvents = new ArrayList<>();
        List<InputEvent> otherRequests = new ArrayList<>();

        while (events.hasNext()) {
            InputEvent event = events.next();
            if (event == null) continue;
            if (event.isRequest() && event.getRequest() != null) {
                int op = event.getRequest().getRequestID() % 10;
                if (op == 1 || op == 2) {
                    addDeleteRequests.add(event);
                } else {
                    otherRequests.add(event);
                }
            } else if (event.isData() && event.getDatapoint() != null) {
                dataEvents.add(event);
            }
        }

        for (InputEvent event : addDeleteRequests) {
            handleRequest(event.getRequest(), routingState, output);
        }
        for (InputEvent event : dataEvents) {
            handleData(event.getDatapoint(), routingState, output);
        }
        for (InputEvent event : otherRequests) {
            handleRequest(event.getRequest(), routingState, output);
        }

        if (routingState.getRegistrations().isEmpty() && routingState.getKeyedParallelism().isEmpty()) {
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
                Map<Integer, Integer> keyed = routingState.getKeyedParallelism();
                keyed.merge(rq.getNoOfP(), 1, Integer::sum);
                LOG.info("DataRouter: registered parallelism={} for uid={} on key={}",
                        rq.getNoOfP(), rq.getUid(), rq.getDataSetKey());

                // Fan out to keyed partition keys (replaces RequestRouter for noOfP>1)
                String baseKey = rq.getDataSetKey();
                for (int i = 0; i < rq.getNoOfP(); i++) {
                    Request copy = new Request(
                            baseKey + "_" + rq.getNoOfP() + "_KEYED_" + i,
                            rq.getRequestID(), rq.getSynopsisID(), rq.getUid(),
                            rq.getStreamID(), rq.getParam(), rq.getNoOfP());
                    output.add(InputEvent.request(copy));
                }
            } else {
                // noOfP=1: forward as-is
                output.add(InputEvent.request(rq));
            }
            routingState.getRegistrations().put(rq.getUid(),
                    new RoutingState.RoutingRegistration(rq.getRequestID(), rq.getNoOfP(), rq.getDataSetKey()));

        } else if (operation == 2) {
            // DELETE: unregister and fan out
            RoutingState.RoutingRegistration reg = routingState.getRegistrations().remove(rq.getUid());
            if (reg != null && reg.getNoOfP() > 1) {
                Map<Integer, Integer> keyed = routingState.getKeyedParallelism();
                int remaining = keyed.getOrDefault(reg.getNoOfP(), 1) - 1;
                if (remaining <= 0) {
                    keyed.remove(reg.getNoOfP());
                } else {
                    keyed.put(reg.getNoOfP(), remaining);
                }
                // Fan out DELETE to keyed partition keys
                String baseKey = reg.getDataSetKey();
                for (int i = 0; i < reg.getNoOfP(); i++) {
                    Request copy = new Request(
                            baseKey + "_" + reg.getNoOfP() + "_KEYED_" + i,
                            rq.getRequestID(), rq.getSynopsisID(), rq.getUid(),
                            rq.getStreamID(), rq.getParam(), rq.getNoOfP());
                    output.add(InputEvent.request(copy));
                }
            } else {
                output.add(InputEvent.request(rq));
            }

        } else {
            // ESTIMATE, etc.
            if (rq.getNoOfP() > 1) {
                // Fan out to keyed partition keys
                String baseKey = rq.getDataSetKey();
                for (int i = 0; i < rq.getNoOfP(); i++) {
                    Request copy = new Request(
                            baseKey + "_" + rq.getNoOfP() + "_KEYED_" + i,
                            rq.getRequestID(), rq.getSynopsisID(), rq.getUid(),
                            rq.getStreamID(), rq.getParam(), rq.getNoOfP());
                    output.add(InputEvent.request(copy));
                }
            } else {
                output.add(InputEvent.request(rq));
            }
        }
    }

    private void handleData(Datapoint datapoint, RoutingState routingState,
                            List<InputEvent> output) {
        Map<Integer, Integer> keyedParallelism = routingState.getKeyedParallelism();

        if (keyedParallelism.isEmpty()) {
            // No parallelism registered — forward data with original key
            output.add(InputEvent.data(datapoint));
            return;
        }

        // For each registered parallelism level, hash and route
        for (int parallelism : keyedParallelism.keySet()) {
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
