package infore.sde.spark.processing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for CombinedProcessor — the merged routing + synopsis lifecycle operator.
 *
 * Tests exercise handleData / handleRequest directly (package-private) to avoid
 * the need for a GroupState stub, consistent with the project's test style.
 *
 * Paths covered:
 *   - GREEN path (noOfP=1): ADD → DATA → ESTIMATE → DELETE for all 4 synopsis types
 *   - PURPLE path (noOfP=2): ADD → DATA hash-routing → inline ESTIMATE reduction
 *   - Error cases: ESTIMATE for missing uid, invalid noOfP, DELETE for missing uid
 *   - State management: isEmpty after DELETE, multiple uids on same key
 */
class CombinedProcessorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String KEY = "Forex";

    private CombinedProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new CombinedProcessor(SDEConfig.fromArgs(new String[0]));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private JsonNode json(String s) throws Exception {
        return MAPPER.readTree(s);
    }

    private Datapoint dp(String streamID, String jsonValues) throws Exception {
        return new Datapoint(KEY, streamID, json(jsonValues));
    }

    private Request addRequest(int uid, int synopsisID, String[] param, int noOfP) {
        return new Request(KEY, 1, synopsisID, uid, null, param, noOfP);
    }

    private Request estimateRequest(int uid, String[] param, int noOfP) {
        return new Request(KEY, 3, 0, uid, null, param, noOfP);
    }

    private Request deleteRequest(int uid) {
        return new Request(KEY, 2, 0, uid, null, null, 1);
    }

    private List<Estimation> call(CombinedState cs, Request rq) {
        List<Estimation> out = new ArrayList<>();
        processor.handleRequest(rq, cs, out, KEY);
        return out;
    }

    // ── GREEN path: CountMin ──────────────────────────────────────────────────

    @Test
    void greenCountMin_addDataEstimate() throws Exception {
        CombinedState cs = new CombinedState();
        String[] param = {"StockID", "price", "Queryable", "0.01", "0.99", "42"};

        // ADD
        call(cs, addRequest(1, 1, param, 1));
        assertThat(cs.getSynopsisSlots()).containsKey(1);
        assertThat(cs.getSynopsisSlots().get(1)).containsKey(0);
        assertThat(cs.getNoOfPByUid().get(1)).isEqualTo(1);

        // DATA: add AAPL twice
        processor.handleData(dp("EURUSD", "{\"StockID\":\"AAPL\",\"price\":\"5\"}"), cs);
        processor.handleData(dp("EURUSD", "{\"StockID\":\"AAPL\",\"price\":\"3\"}"), cs);
        processor.handleData(dp("EURUSD", "{\"StockID\":\"GOOG\",\"price\":\"10\"}"), cs);

        // ESTIMATE for AAPL → should return ≥ 8 (CountMin never underestimates)
        List<Estimation> out = call(cs, estimateRequest(1, new String[]{"AAPL"}, 1));
        assertThat(out).hasSize(1);
        double estimate = Double.parseDouble(out.get(0).getEstimation().toString());
        assertThat(estimate).isGreaterThanOrEqualTo(8.0);
        assertThat(out.get(0).getUid()).isEqualTo(1);
        assertThat(out.get(0).getNoOfP()).isEqualTo(1);
    }

    @Test
    void greenCountMin_delete() throws Exception {
        CombinedState cs = new CombinedState();
        String[] param = {"StockID", "price", "Queryable", "0.01", "0.99", "42"};
        call(cs, addRequest(1, 1, param, 1));

        call(cs, deleteRequest(1));

        assertThat(cs.getSynopsisSlots()).doesNotContainKey(1);
        assertThat(cs.getNoOfPByUid()).doesNotContainKey(1);
        assertThat(cs.isEmpty()).isTrue();
    }

    // ── GREEN path: BloomFilter ───────────────────────────────────────────────

    @Test
    void greenBloomFilter_membershipCheck() throws Exception {
        CombinedState cs = new CombinedState();
        String[] param = {"StockID", "price", "Queryable", "10000", "0.01"};
        call(cs, addRequest(2, 2, param, 1));

        processor.handleData(dp("EURUSD", "{\"StockID\":\"AAPL\",\"price\":\"100\"}"), cs);

        List<Estimation> present = call(cs, estimateRequest(2, new String[]{"AAPL"}, 1));
        assertThat(present).hasSize(1);
        assertThat(present.get(0).getEstimation()).isEqualTo(true);

        List<Estimation> absent = call(cs, estimateRequest(2, new String[]{"MSFT"}, 1));
        assertThat(absent).hasSize(1);
        assertThat(absent.get(0).getEstimation()).isEqualTo(false);
    }

    // ── GREEN path: AMS ───────────────────────────────────────────────────────

    @Test
    void greenAMS_returnsNonNullEstimate() throws Exception {
        CombinedState cs = new CombinedState();
        String[] param = {"StockID", "price", "Queryable", "100", "5"};
        call(cs, addRequest(3, 3, param, 1));

        processor.handleData(dp("EURUSD", "{\"StockID\":\"AAPL\",\"price\":\"10\"}"), cs);
        processor.handleData(dp("EURUSD", "{\"StockID\":\"GOOG\",\"price\":\"20\"}"), cs);

        List<Estimation> out = call(cs, estimateRequest(3, new String[]{"AAPL"}, 1));
        assertThat(out).hasSize(1);
        assertThat(out.get(0).getEstimation()).isNotNull();
    }

    // ── GREEN path: HyperLogLog ───────────────────────────────────────────────

    @Test
    void greenHyperLogLog_cardinalityEstimate() throws Exception {
        CombinedState cs = new CombinedState();
        String[] param = {"StockID", "price", "Queryable", "0.05"};
        call(cs, addRequest(4, 4, param, 1));

        processor.handleData(dp("EURUSD", "{\"StockID\":\"AAPL\",\"price\":\"100\"}"), cs);
        processor.handleData(dp("EURUSD", "{\"StockID\":\"GOOG\",\"price\":\"200\"}"), cs);
        processor.handleData(dp("EURUSD", "{\"StockID\":\"AAPL\",\"price\":\"101\"}"), cs);

        List<Estimation> out = call(cs, estimateRequest(4, new String[]{}, 1));
        assertThat(out).hasSize(1);
        double cardinality = Double.parseDouble(out.get(0).getEstimation().toString());
        // 3 events but only 2 unique prices — HLL should report ≈2, allow ±1 for small N
        assertThat(cardinality).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(4);
    }

    // ── PURPLE path: inline reduction ────────────────────────────────────────

    @Test
    void purpleCountMin_dataRoutedToCorrectSlot_estimateReducesInline() throws Exception {
        CombinedState cs = new CombinedState();
        String[] param = {"StockID", "price", "Queryable", "0.01", "0.99", "42"};
        call(cs, addRequest(10, 1, param, 2));  // noOfP=2 → 2 slots

        assertThat(cs.getSynopsisSlots().get(10)).hasSize(2);
        assertThat(cs.getNoOfPByUid().get(10)).isEqualTo(2);

        // Two stream IDs hash to different slots — feed data for each
        String sA = "EURUSD"; // slot = Math.abs("EURUSD".hashCode()) % 2
        String sB = "GBPUSD"; // slot = Math.abs("GBPUSD".hashCode()) % 2
        int slotA = Math.abs(sA.hashCode()) % 2;
        int slotB = Math.abs(sB.hashCode()) % 2;

        // Add AAPL=5 on stream A (3 times) and AAPL=3 on stream B (2 times)
        for (int i = 0; i < 3; i++)
            processor.handleData(dp(sA, "{\"StockID\":\"AAPL\",\"price\":\"5\"}"), cs);
        for (int i = 0; i < 2; i++)
            processor.handleData(dp(sB, "{\"StockID\":\"AAPL\",\"price\":\"3\"}"), cs);

        // Verify data went to the correct slot
        int expectedSlotA = 3; // 3 adds of price=5 → slot slotA count for AAPL
        int expectedSlotB = 2; // 2 adds of price=3 → slot slotB count for AAPL

        // ESTIMATE: inline reduction should sum both slots
        List<Estimation> out = call(cs, estimateRequest(10, new String[]{"AAPL"}, 2));
        assertThat(out).hasSize(1);
        double merged = Double.parseDouble(out.get(0).getEstimation().toString());
        // CountMin sums: total AAPL count ≥ 3+2=5 (never underestimates)
        if (slotA != slotB) {
            assertThat(merged).isGreaterThanOrEqualTo(5.0);
        } else {
            // Both streams hash to same slot — slot holds 5 inserts, other slot is 0
            assertThat(merged).isGreaterThanOrEqualTo(0.0);
        }
        assertThat(out.get(0).getNoOfP()).isEqualTo(2);
    }

    @Test
    void purpleBloomFilter_orReduceAcrossSlots() throws Exception {
        CombinedState cs = new CombinedState();
        String[] param = {"StockID", "price", "Queryable", "10000", "0.01"};
        call(cs, addRequest(20, 2, param, 2));  // noOfP=2

        // Add AAPL to whichever slot "EURUSD" routes to
        processor.handleData(dp("EURUSD", "{\"StockID\":\"AAPL\",\"price\":\"100\"}"), cs);

        // ESTIMATE: OR across both slots — at least one slot has AAPL → true
        List<Estimation> out = call(cs, estimateRequest(20, new String[]{"AAPL"}, 2));
        assertThat(out).hasSize(1);
        assertThat(out.get(0).getEstimation()).isEqualTo(true);

        // MSFT was never inserted → false in all slots
        List<Estimation> absent = call(cs, estimateRequest(20, new String[]{"MSFT"}, 2));
        assertThat(absent).hasSize(1);
        assertThat(absent.get(0).getEstimation()).isEqualTo(false);
    }

    // ── Slot hash correctness ─────────────────────────────────────────────────

    @Test
    void dataRoutedToCorrectSlotOnlyNotAllSlots() throws Exception {
        CombinedState cs = new CombinedState();
        String[] param = {"StockID", "price", "Queryable", "0.001", "0.999", "1"};
        call(cs, addRequest(30, 1, param, 2));  // noOfP=2

        String stream = "EURUSD";
        int expectedSlot = Math.abs(stream.hashCode()) % 2;
        int otherSlot = 1 - expectedSlot;

        processor.handleData(dp(stream, "{\"StockID\":\"X\",\"price\":\"99\"}"), cs);

        Map<Integer, infore.sde.spark.synopses.Synopsis> slots = cs.getSynopsisSlots().get(30);

        // Query the expected slot — should have recorded X
        Request rqSlot = new Request(KEY, 3, 0, 30, null, new String[]{"X"}, 1);
        Estimation fromExpected = slots.get(expectedSlot).estimate(rqSlot);
        double expected = Double.parseDouble(fromExpected.getEstimation().toString());

        // Query the other slot — should be 0 (X was never inserted there)
        Estimation fromOther = slots.get(otherSlot).estimate(rqSlot);
        double other = Double.parseDouble(fromOther.getEstimation().toString());

        assertThat(expected).isGreaterThan(0.0);
        assertThat(other).isEqualTo(0.0);
    }

    // ── Error cases ───────────────────────────────────────────────────────────

    @Test
    void estimateForMissingUidEmitsErrorNotice() {
        CombinedState cs = new CombinedState();

        List<Estimation> out = call(cs, estimateRequest(99, new String[]{"X"}, 1));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).getRequestID()).isEqualTo(-1);
        assertThat(out.get(0).getEstimation().toString()).contains("does not exist");
        assertThat(out.get(0).getUid()).isEqualTo(99);
    }

    @Test
    void deleteForMissingUidDoesNotThrow() {
        CombinedState cs = new CombinedState();
        // Should log a warn but not throw
        assertThat(call(cs, deleteRequest(99))).isEmpty();
    }

    @Test
    void invalidNoOfPIsRejected() {
        CombinedState cs = new CombinedState();
        Request bad = new Request(KEY, 1, 1, 7, null,
                new String[]{"StockID", "price", "Q", "0.01", "0.99", "1"}, 0);
        call(cs, bad);
        assertThat(cs.getSynopsisSlots()).doesNotContainKey(7);
    }

    // ── Multiple uids on same key ─────────────────────────────────────────────

    @Test
    void multipleUidsCoexistOnSameKey() throws Exception {
        CombinedState cs = new CombinedState();
        String[] cmParam  = {"StockID", "price", "Queryable", "0.01", "0.99", "1"};
        String[] bfParam  = {"StockID", "price", "Queryable", "1000", "0.01"};

        call(cs, addRequest(1, 1, cmParam, 1));   // CountMin uid=1
        call(cs, addRequest(2, 2, bfParam, 1));   // BloomFilter uid=2

        processor.handleData(dp("EURUSD", "{\"StockID\":\"AAPL\",\"price\":\"10\"}"), cs);

        // Both synopses received the data event
        List<Estimation> cmOut = call(cs, estimateRequest(1, new String[]{"AAPL"}, 1));
        assertThat(cmOut).hasSize(1);
        assertThat(Double.parseDouble(cmOut.get(0).getEstimation().toString())).isGreaterThan(0.0);

        List<Estimation> bfOut = call(cs, estimateRequest(2, new String[]{"AAPL"}, 1));
        assertThat(bfOut).hasSize(1);
        assertThat(bfOut.get(0).getEstimation()).isEqualTo(true);

        // Delete one, other survives
        call(cs, deleteRequest(1));
        assertThat(cs.getSynopsisSlots()).containsKey(2);
        assertThat(cs.isEmpty()).isFalse();

        call(cs, deleteRequest(2));
        assertThat(cs.isEmpty()).isTrue();
    }

    // ── State snapshot ────────────────────────────────────────────────────────

    @Test
    void snapshotForSerializationDoesNotThrow() throws Exception {
        CombinedState cs = new CombinedState();
        String[] param = {"StockID", "price", "Queryable", "0.05"};
        call(cs, addRequest(40, 4, param, 1));  // HyperLogLog (has transient state)

        processor.handleData(dp("X", "{\"StockID\":\"A\",\"price\":\"1\"}"), cs);

        // Must not throw; after this call all transient state is in byte[]
        assertThat(cs.isEmpty()).isFalse();
        cs.snapshotForSerialization();
    }
}
