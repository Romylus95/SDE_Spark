package infore.sde.spark.aggregation;

import infore.sde.spark.messages.Estimation;
import infore.sde.spark.reduceFunctions.ReduceFunction;
import infore.sde.spark.reduceFunctions.SimpleORFunction;
import infore.sde.spark.reduceFunctions.SimpleSumFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Layer 5 — Aggregation (PURPLE path only).
 *
 * Stateless within-batch aggregation: all N partial Estimations for a given uid
 * are produced in the same micro-batch (same input triggers all N PURPLE slots),
 * so flatMapGroups collects and merges them without any cross-batch state.
 *
 * Eliminates the 40-partition HDFS state store of the previous flatMapGroupsWithState
 * implementation — no checkpoint writes for aggregation state.
 *
 * If fewer than noOfP partials arrive in a batch (e.g. a PURPLE slot had no
 * registered synopsis for that uid), the incomplete group is silently dropped.
 */
public class ReduceAggregator implements FlatMapGroupsFunction<Integer, Estimation, Estimation> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ReduceAggregator.class);

    @Override
    public Iterator<Estimation> call(Integer uid, Iterator<Estimation> partials) {
        List<Estimation> output = new ArrayList<>();

        ReduceFunction reducer = null;
        Estimation template = null;

        while (partials.hasNext()) {
            Estimation partial = partials.next();

            if (reducer == null) {
                reducer = createReducer(partial);
                template = new Estimation(partial);
            }

            if (reducer.add(partial)) {
                Object finalResult = reducer.reduce();
                Estimation result = new Estimation(template);
                result.setEstimation(finalResult);
                output.add(result);
                LOG.debug("Aggregation complete for uid={}: {}", uid, finalResult);
                // Reset for next ESTIMATE group within same batch
                reducer = null;
                template = null;
            }
        }

        if (reducer != null) {
            // Incomplete group — fewer partials than noOfP arrived in this batch
            LOG.debug("uid={}: incomplete partial group in batch, dropping", uid);
        }

        return output.iterator();
    }

    private ReduceFunction createReducer(Estimation estimation) {
        int synopsisID = estimation.getSynopsisID();
        int noOfP      = estimation.getNoOfP();

        switch (synopsisID) {
            case 2:
                return new SimpleORFunction(noOfP, 0, synopsisID, estimation.getRequestID());
            case 1: case 3: case 4:
            default:
                return new SimpleSumFunction(noOfP, 0, synopsisID, estimation.getRequestID());
        }
    }
}
