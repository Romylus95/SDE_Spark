package infore.sde.spark.routing;

import infore.sde.spark.messages.Request;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Layer 2 — RequestRouter fan-out logic.
 * No Spark or Kafka needed.
 */
class RequestRouterTest {

    private final RequestRouter router = new RequestRouter();

    @Test
    void singlePartitionPassesThrough() throws Exception {
        Request rq = new Request("Forex", 1, 1, 42, "EURUSD",
                new String[]{"StockID", "price", "Queryable", "0.01", "0.99", "1"}, 1);

        List<Request> result = collect(router.call(rq));

        assertEquals(1, result.size());
        assertEquals("Forex", result.get(0).getDataSetKey());
        assertEquals(42, result.get(0).getUid());
    }

    @Test
    void multiPartitionFansOut() throws Exception {
        Request rq = new Request("Forex", 1, 1, 42, "EURUSD",
                new String[]{"StockID", "price", "Queryable", "0.01", "0.99", "1"}, 3);

        List<Request> result = collect(router.call(rq));

        assertEquals(3, result.size());
        assertEquals("Forex_3_KEYED_0", result.get(0).getDataSetKey());
        assertEquals("Forex_3_KEYED_1", result.get(1).getDataSetKey());
        assertEquals("Forex_3_KEYED_2", result.get(2).getDataSetKey());

        // All copies share the same uid, synopsisID, noOfP
        for (Request r : result) {
            assertEquals(42, r.getUid());
            assertEquals(1, r.getSynopsisID());
            assertEquals(3, r.getNoOfP());
        }
    }

    @Test
    void deleteRequestFansOut() throws Exception {
        Request rq = new Request("Forex", 2, 1, 42, "EURUSD",
                new String[]{}, 2);

        List<Request> result = collect(router.call(rq));

        assertEquals(2, result.size());
        assertEquals("Forex_2_KEYED_0", result.get(0).getDataSetKey());
        assertEquals("Forex_2_KEYED_1", result.get(1).getDataSetKey());
    }

    @Test
    void estimateRequestFansOut() throws Exception {
        Request rq = new Request("Forex", 3, 1, 42, "EURUSD",
                new String[]{"AAPL"}, 2);

        List<Request> result = collect(router.call(rq));

        assertEquals(2, result.size());
    }

    @Test
    void requestIdAbove8IsDropped() throws Exception {
        Request rq = new Request("Forex", 8, 1, 42, "EURUSD",
                new String[]{}, 1);

        List<Request> result = collect(router.call(rq));

        assertEquals(0, result.size());
    }

    @Test
    void randomAddFansOutWithKeyedPrefix() throws Exception {
        // requestID=4 is ADD_RANDOM, but our router uses KEYED prefix for all
        Request rq = new Request("Forex", 4, 1, 42, "EURUSD",
                new String[]{"StockID", "price", "Queryable", "0.01", "0.99", "1"}, 2);

        List<Request> result = collect(router.call(rq));

        assertEquals(2, result.size());
        assertTrue(result.get(0).getDataSetKey().contains("_KEYED_"));
    }

    private <T> List<T> collect(Iterator<T> iter) {
        List<T> list = new ArrayList<>();
        iter.forEachRemaining(list::add);
        return list;
    }
}
