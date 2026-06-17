package infore.sde.spark.routing;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Request;
import infore.sde.spark.processing.InputEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for StatelessRouter — the stateless fan-out layer in the hybrid architecture.
 *
 * Verifies:
 *  - numSlots=1: events pass through unchanged (no routing overhead)
 *  - numSlots=N: data routed to exactly one KEYED slot per event
 *  - numSlots=N: same streamID always routes to same slot (deterministic hash)
 *  - numSlots=N: requests fanned out to exactly N copies
 *  - numSlots=N: fan-out requests carry noOfP=numSlots
 *  - numSlots=N: routing key format is correct
 *  - data and request events preserve original dataSetKey
 */
class StatelessRouterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BASE_KEY = "Forex";

    private Datapoint dp(String streamID) throws Exception {
        return MAPPER.readValue(
                "{\"dataSetkey\":\"" + BASE_KEY + "\",\"streamID\":\"" + streamID +
                "\",\"values\":{\"StockID\":\"AAPL\",\"price\":\"100\"}}",
                Datapoint.class);
    }

    private Request addRequest(int uid, int noOfP) {
        return new Request(BASE_KEY, 1, 1, uid, null,
                new String[]{"StockID", "price", "Q", "0.01", "0.99", "1"}, noOfP);
    }

    private Request estimateRequest(int uid, int noOfP) {
        return new Request(BASE_KEY, 3, 0, uid, null, new String[]{"AAPL"}, noOfP);
    }

    private List<InputEvent> route(StatelessRouter router, InputEvent event) throws Exception {
        List<InputEvent> out = new ArrayList<>();
        Iterator<InputEvent> it = router.call(event);
        while (it.hasNext()) out.add(it.next());
        return out;
    }

    // ── numSlots=1: pass-through ──────────────────────────────────────────────

    @Test
    void numSlots1_dataPassesThroughUnchanged() throws Exception {
        StatelessRouter router = new StatelessRouter(1);
        InputEvent event = InputEvent.data(dp("EURUSD"));

        List<InputEvent> out = route(router, event);

        assertThat(out).hasSize(1);
        assertThat(out.get(0)).isSameAs(event);
        assertThat(out.get(0).getRoutingKey()).isEqualTo(BASE_KEY);
        assertThat(out.get(0).getDataSetKey()).isEqualTo(BASE_KEY);
    }

    @Test
    void numSlots1_requestPassesThroughUnchanged() throws Exception {
        StatelessRouter router = new StatelessRouter(1);
        InputEvent event = InputEvent.request(addRequest(42, 1));

        List<InputEvent> out = route(router, event);

        assertThat(out).hasSize(1);
        assertThat(out.get(0)).isSameAs(event);
        assertThat(out.get(0).getRoutingKey()).isEqualTo(BASE_KEY);
    }

    // ── numSlots=4: data routing ──────────────────────────────────────────────

    @Test
    void numSlots4_dataRoutesToExactlyOneSlot() throws Exception {
        StatelessRouter router = new StatelessRouter(4);
        InputEvent event = InputEvent.data(dp("EURUSD"));

        List<InputEvent> out = route(router, event);

        assertThat(out).hasSize(1);
    }

    @Test
    void numSlots4_dataRoutingKeyHasCorrectFormat() throws Exception {
        StatelessRouter router = new StatelessRouter(4);
        InputEvent event = InputEvent.data(dp("EURUSD"));

        List<InputEvent> out = route(router, event);
        String routingKey = out.get(0).getRoutingKey();

        assertThat(routingKey).startsWith(BASE_KEY + "_4_KEYED_");
        int slot = Integer.parseInt(routingKey.substring(routingKey.lastIndexOf("_") + 1));
        assertThat(slot).isBetween(0, 3);
    }

    @Test
    void numSlots4_sameStreamIDAlwaysRoutesToSameSlot() throws Exception {
        StatelessRouter router = new StatelessRouter(4);

        Set<String> routingKeys = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            List<InputEvent> out = route(router, InputEvent.data(dp("EURUSD")));
            routingKeys.add(out.get(0).getRoutingKey());
        }

        assertThat(routingKeys).hasSize(1);
    }

    @Test
    void numSlots4_dataPreservesBaseDataSetKey() throws Exception {
        StatelessRouter router = new StatelessRouter(4);
        InputEvent event = InputEvent.data(dp("EURUSD"));

        List<InputEvent> out = route(router, event);

        // getDataSetKey() still returns the original base key
        assertThat(out.get(0).getDataSetKey()).isEqualTo(BASE_KEY);
    }

    @Test
    void numSlots4_differentStreamsMayRouteToDifferentSlots() throws Exception {
        StatelessRouter router = new StatelessRouter(4);

        // With 4 slots and many streams, different streams should route to different slots
        Set<String> seen = new HashSet<>();
        String[] streams = {"EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCHF", "USDCAD"};
        for (String s : streams) {
            List<InputEvent> out = route(router, InputEvent.data(dp(s)));
            seen.add(out.get(0).getRoutingKey());
        }
        // With 6 streams across 4 slots, at least 2 distinct slots should be used
        assertThat(seen.size()).isGreaterThanOrEqualTo(2);
    }

    // ── numSlots=4: request fan-out ───────────────────────────────────────────

    @Test
    void numSlots4_addRequestFansOutToNSlots() throws Exception {
        StatelessRouter router = new StatelessRouter(4);
        InputEvent event = InputEvent.request(addRequest(42, 1));

        List<InputEvent> out = route(router, event);

        assertThat(out).hasSize(4);
    }

    @Test
    void numSlots4_fanOutCoversAllSlots() throws Exception {
        StatelessRouter router = new StatelessRouter(4);
        InputEvent event = InputEvent.request(addRequest(42, 1));

        List<InputEvent> out = route(router, event);
        Set<String> routingKeys = new HashSet<>();
        for (InputEvent e : out) routingKeys.add(e.getRoutingKey());

        for (int slot = 0; slot < 4; slot++) {
            assertThat(routingKeys).contains(BASE_KEY + "_4_KEYED_" + slot);
        }
    }

    @Test
    void numSlots4_fanOutRequestHasNoOfPEqualToNumSlots() throws Exception {
        StatelessRouter router = new StatelessRouter(4);
        InputEvent event = InputEvent.request(addRequest(42, 1));

        List<InputEvent> out = route(router, event);

        for (InputEvent e : out) {
            assertThat(e.getRequest().getNoOfP()).isEqualTo(4);
        }
    }

    @Test
    void numSlots4_fanOutPreservesBaseDataSetKey() throws Exception {
        StatelessRouter router = new StatelessRouter(4);
        InputEvent event = InputEvent.request(estimateRequest(42, 1));

        List<InputEvent> out = route(router, event);

        for (InputEvent e : out) {
            assertThat(e.getDataSetKey()).isEqualTo(BASE_KEY);
            assertThat(e.getRequest().getDataSetKey()).isEqualTo(BASE_KEY);
        }
    }

    @Test
    void numSlots4_estimateRequestFansOutToNSlots() throws Exception {
        StatelessRouter router = new StatelessRouter(4);
        InputEvent event = InputEvent.request(estimateRequest(10, 1));

        List<InputEvent> out = route(router, event);

        assertThat(out).hasSize(4);
        for (InputEvent e : out) {
            assertThat(e.getRequest().getNoOfP()).isEqualTo(4);
        }
    }

    // ── Slot computation correctness ──────────────────────────────────────────

    @Test
    void slotComputationMatchesHashFormula() throws Exception {
        StatelessRouter router = new StatelessRouter(4);
        String streamID = "EURUSD";
        int expectedSlot = Math.abs(streamID.hashCode()) % 4;

        List<InputEvent> out = route(router, InputEvent.data(dp(streamID)));
        String routingKey = out.get(0).getRoutingKey();
        int actualSlot = Integer.parseInt(routingKey.substring(routingKey.lastIndexOf("_") + 1));

        assertThat(actualSlot).isEqualTo(expectedSlot);
    }
}
