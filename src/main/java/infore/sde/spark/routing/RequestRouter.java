package infore.sde.spark.routing;

import infore.sde.spark.messages.Request;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Layer 2 — Request fan-out.
 * Expands a single request into N partition-specific copies when noOfP > 1.
 * Each copy is stamped with a unique routing key: "{dataSetKey}_{noOfP}_KEYED_{i}"
 *
 * Replaces: RqRouterFlatMap.java from Flink
 */
public class RequestRouter implements FlatMapFunction<Request, Request> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RequestRouter.class);

    @Override
    public Iterator<Request> call(Request rq) throws Exception {
        List<Request> expanded = new ArrayList<>();

        int operation = rq.getRequestID() % 10;
        if (operation >= 8) {
            return expanded.iterator();
        }

        if (rq.getNoOfP() == 1) {
            expanded.add(rq);
        } else {
            String baseKey = rq.getDataSetKey();
            for (int i = 0; i < rq.getNoOfP(); i++) {
                Request copy = new Request(
                        baseKey + "_" + rq.getNoOfP() + "_KEYED_" + i,
                        rq.getRequestID(),
                        rq.getSynopsisID(),
                        rq.getUid(),
                        rq.getStreamID(),
                        rq.getParam(),
                        rq.getNoOfP()
                );
                expanded.add(copy);
            }
            LOG.debug("Fan-out request uid={} into {} copies", rq.getUid(), rq.getNoOfP());
        }

        return expanded.iterator();
    }
}
