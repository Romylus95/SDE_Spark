package infore.sde.spark.processing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import infore.sde.spark.synopses.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for synopsis add/estimate operations.
 * These test the algorithms directly, independent of Spark.
 */
class SynopsisProcessorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void countMinAddAndEstimate() throws Exception {
        String[] params = {"StockID", "price", "Queryable", "0.01", "0.99", "42"};
        CountMin cm = new CountMin(1, params);

        cm.add(createJsonNode("{\"StockID\": \"AAPL\", \"price\": \"1\"}"));
        cm.add(createJsonNode("{\"StockID\": \"AAPL\", \"price\": \"1\"}"));
        cm.add(createJsonNode("{\"StockID\": \"GOOG\", \"price\": \"1\"}"));

        Request rq = new Request("Forex", 3, 1, 1, "EURUSD",
                new String[]{"AAPL"}, 1);
        Estimation est = cm.estimate(rq);

        assertNotNull(est);
        assertNotNull(est.getEstimation());
        double count = Double.parseDouble(est.getEstimation().toString());
        assertEquals(2.0, count, 0.1);
    }

    @Test
    void bloomFilterAddAndEstimate() throws Exception {
        String[] params = {"StockID", "price", "Queryable", "1000", "0.01"};
        Bloomfilter bf = new Bloomfilter(2, params);

        bf.add(createJsonNode("{\"StockID\": \"AAPL\", \"price\": \"182.50\"}"));
        bf.add(createJsonNode("{\"StockID\": \"GOOG\", \"price\": \"141.20\"}"));

        Request rqPresent = new Request("Forex", 3, 2, 2, "EURUSD",
                new String[]{"AAPL"}, 1);
        Estimation estPresent = bf.estimate(rqPresent);
        assertEquals(true, estPresent.getEstimation());

        Request rqAbsent = new Request("Forex", 3, 2, 2, "EURUSD",
                new String[]{"MSFT"}, 1);
        Estimation estAbsent = bf.estimate(rqAbsent);
        assertEquals(false, estAbsent.getEstimation());
    }

    @Test
    void hyperLogLogAddAndEstimate() throws Exception {
        String[] params = {"StockID", "price", "Queryable", "0.05"};
        HyperLogLogSynopsis hll = new HyperLogLogSynopsis(4, params);

        hll.add(createJsonNode("{\"StockID\": \"AAPL\", \"price\": \"182.50\"}"));
        hll.add(createJsonNode("{\"StockID\": \"GOOG\", \"price\": \"141.20\"}"));
        hll.add(createJsonNode("{\"StockID\": \"AAPL\", \"price\": \"183.00\"}"));

        Request rq = new Request("Forex", 3, 4, 4, "EURUSD",
                new String[]{"StockID"}, 1);
        Estimation est = hll.estimate(rq);

        assertNotNull(est);
        double cardinality = Double.parseDouble(est.getEstimation().toString());
        // HLL should report ~2 unique values (AAPL's price varies but we track by price field)
        assertTrue(cardinality >= 1 && cardinality <= 4);
    }

    @Test
    void amsAddAndEstimate() throws Exception {
        String[] params = {"StockID", "price", "Queryable", "100", "5"};
        AMSsynopsis ams = new AMSsynopsis(3, params);

        ams.add(createJsonNode("{\"StockID\": \"AAPL\", \"price\": \"182.50\"}"));
        ams.add(createJsonNode("{\"StockID\": \"GOOG\", \"price\": \"141.20\"}"));

        Request rq = new Request("Forex", 3, 3, 3, "EURUSD",
                new String[]{"AAPL"}, 1);
        Estimation est = ams.estimate(rq);

        assertNotNull(est);
    }

    private JsonNode createJsonNode(String json) throws Exception {
        return MAPPER.readTree(json);
    }
}
