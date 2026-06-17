package infore.sde.spark.routing;

import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Request;
import infore.sde.spark.processing.InputEvent;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Layer 2 — Stateless fan-out router.
 *
 * Replaces the stateful DataRouter with a pure function keyed on numSlots (a
 * job-level parameter passed at spark-submit time via --num-slots). Because
 * numSlots is fixed for the lifetime of the job, no routing state needs to be
 * checkpointed: the fan-out logic is entirely deterministic.
 *
 * Data events:   routed to exactly ONE slot  → dataSetKey + "_" + numSlots + "_KEYED_" + slot
 * Request events: fanned out to ALL numSlots slots with noOfP set to numSlots
 *
 * When numSlots == 1, events pass through unchanged (original dataSetKey used),
 * which is equivalent to the old GREEN path and requires no aggregation.
 *
 * Checkpoint savings vs baseline DataRouter:
 *   DataRouter was a flatMapGroupsWithState → spark.sql.shuffle.partitions state files/batch
 *   StatelessRouter is a flatMap → zero state files/batch
 */
public class StatelessRouter implements FlatMapFunction<InputEvent, InputEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(StatelessRouter.class);

    private final int numSlots;

    public StatelessRouter(int numSlots) {
        this.numSlots = numSlots;
    }

    @Override
    public Iterator<InputEvent> call(InputEvent event) {
        if (numSlots <= 1) {
            return Collections.singletonList(event).iterator();
        }

        String baseKey = event.getDataSetKey();
        List<InputEvent> out = new ArrayList<>();

        if (event.isData()) {
            Datapoint dp = event.getDatapoint();
            int slot = Math.abs(dp.getStreamID().hashCode()) % numSlots;
            String routedKey = baseKey + "_" + numSlots + "_KEYED_" + slot;
            out.add(InputEvent.dataRouted(dp, routedKey));
        } else {
            // ADD / ESTIMATE / DELETE: fan out to all slots so each SynopsisProcessor
            // instance sees the request and acts on its local synopsis copy.
            Request rq = event.getRequest();
            Request fanRq = withNumSlots(rq, numSlots);
            for (int slot = 0; slot < numSlots; slot++) {
                String routedKey = baseKey + "_" + numSlots + "_KEYED_" + slot;
                out.add(InputEvent.requestRouted(fanRq, routedKey));
            }
        }

        return out.iterator();
    }

    /** Returns a copy of the request with noOfP set to numSlots. */
    private static Request withNumSlots(Request rq, int numSlots) {
        return new Request(rq.getDataSetKey(), rq.getRequestID(), rq.getSynopsisID(),
                rq.getUid(), rq.getStreamID(), rq.getParam(), numSlots);
    }

    public int getNumSlots() { return numSlots; }
}
