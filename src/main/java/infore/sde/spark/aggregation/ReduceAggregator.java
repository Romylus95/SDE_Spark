package infore.sde.spark.aggregation;

import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.reduceFunctions.ReduceFunction;
import infore.sde.spark.reduceFunctions.SimpleORFunction;
import infore.sde.spark.reduceFunctions.SimpleSumFunction;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Layer 5 — Aggregation (PURPLE path only).
 * Collects partial estimation results from multiple parallel synopsis partitions
 * and merges them into a single final estimation.
 *
 * Grouped by uid so all partials for the same synopsis instance land here.
 * Uses flatMapGroupsWithState to buffer partials until count == noOfP.
 *
 * Reduce strategy per synopsis type:
 *   - CountMin (1), AMS (3), HyperLogLog (4) -> SimpleSumFunction (sum partial estimates)
 *   - BloomFilter (2) -> SimpleORFunction (OR partial membership results)
 *
 * Supports processing-time timeout to discard incomplete aggregations
 * if not all partials arrive within the configured timeout window.
 */
public class ReduceAggregator
        implements FlatMapGroupsWithStateFunction<Integer, Estimation, AggregationState, Estimation> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ReduceAggregator.class);

    private final SDEConfig config;

    public ReduceAggregator(SDEConfig config) {
        this.config = config;
    }

    @Override
    public Iterator<Estimation> call(Integer uid, Iterator<Estimation> partials,
                                     GroupState<AggregationState> state) throws Exception {

        List<Estimation> output = new ArrayList<>();

        // Handle timeout — discard incomplete aggregation
        if (state.hasTimedOut()) {
            LOG.warn("Aggregation timeout for uid={}, discarding incomplete partials", uid);
            state.remove();
            return output.iterator();
        }

        AggregationState aggState;
        if (state.exists()) {
            aggState = state.get();
        } else {
            aggState = new AggregationState();
        }

        while (partials.hasNext()) {
            Estimation partial = partials.next();

            ReduceFunction reducer = aggState.getReducer();
            if (reducer == null) {
                reducer = createReducer(partial);
                aggState.setReducer(reducer);
                aggState.setTemplate(new Estimation(partial));
            }

            if (reducer.add(partial)) {
                // All partials collected — reduce and emit
                Object finalResult = reducer.reduce();
                Estimation result = new Estimation(aggState.getTemplate());
                result.setEstimation(finalResult);
                output.add(result);
                LOG.debug("Aggregation complete for uid={}: {}", uid, finalResult);
                state.remove();
                return output.iterator();
            }
        }

        // Not yet complete — save state and wait for more
        state.update(aggState);
        state.setTimeoutDuration(config.getAggregationTimeout().toMillis());
        return output.iterator();
    }

    private ReduceFunction createReducer(Estimation estimation) {
        int synopsisID = estimation.getSynopsisID();
        int noOfP = estimation.getNoOfP();

        return switch (synopsisID) {
            case 1, 3, 4 -> new SimpleSumFunction(noOfP, 0, estimation.getParam(),
                    synopsisID, estimation.getRequestID());
            case 2 -> new SimpleORFunction(noOfP, 0, estimation.getParam(),
                    synopsisID, estimation.getRequestID());
            default -> {
                LOG.warn("Unknown synopsisID={} for aggregation, using SimpleSumFunction", synopsisID);
                yield new SimpleSumFunction(noOfP, 0, estimation.getParam(),
                        synopsisID, estimation.getRequestID());
            }
        };
    }
}
