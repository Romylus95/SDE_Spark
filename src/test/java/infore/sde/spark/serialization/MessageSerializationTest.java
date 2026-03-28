package infore.sde.spark.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that JSON wire format matches the Flink SDE contract exactly.
 * The @JsonProperty annotations must preserve original field naming.
 */
class MessageSerializationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void datapointDeserializesFromFlinkFormat() throws Exception {
        String json = """
                {
                  "dataSetkey": "Forex",
                  "streamID": "EURUSD",
                  "values": { "StockID": "AAPL", "price": "182.50" }
                }
                """;

        Datapoint dp = MAPPER.readValue(json, Datapoint.class);

        assertEquals("Forex", dp.getDataSetKey());
        assertEquals("EURUSD", dp.getStreamID());
        assertEquals("AAPL", dp.getValues().get("StockID").asText());
    }

    @Test
    void requestDeserializesFromFlinkFormat() throws Exception {
        String json = """
                {
                  "dataSetkey": "Forex",
                  "requestID": 1,
                  "synopsisID": 1,
                  "uid": 42,
                  "streamID": "INTEL",
                  "param": ["StockID", "price", "Queryable", "0.01", "4"],
                  "noOfP": 4
                }
                """;

        Request rq = MAPPER.readValue(json, Request.class);

        assertEquals("Forex", rq.getDataSetKey());
        assertEquals(1, rq.getRequestID());
        assertEquals(1, rq.getSynopsisID());
        assertEquals(42, rq.getUid());
        assertEquals(4, rq.getNoOfP());
        assertEquals("StockID", rq.getParam()[0]);
    }

    @Test
    void estimationSerializesToFlinkFormat() throws Exception {
        Estimation est = new Estimation(42, "Forex_42", 3, 1,
                "Forex", 2, new String[]{"StockID", "price"}, 1);
        est.setStreamID("EURUSD");

        String json = MAPPER.writeValueAsString(est);

        assertTrue(json.contains("\"uid\":42"));
        assertTrue(json.contains("\"estimationkey\":\"Forex_42\""));
        assertTrue(json.contains("\"synopsisID\":1"));
        assertTrue(json.contains("\"estimation\":2"));
        assertTrue(json.contains("\"noOfP\":1"));
    }

    @Test
    void datapointRoundTrip() throws Exception {
        String original = """
                {"dataSetkey":"Forex","streamID":"EURUSD","values":{"StockID":"AAPL","price":"182.50"}}
                """;

        Datapoint dp = MAPPER.readValue(original, Datapoint.class);
        String reserialized = MAPPER.writeValueAsString(dp);

        // Re-parse to compare field values (order may differ)
        Datapoint reparsed = MAPPER.readValue(reserialized, Datapoint.class);
        assertEquals(dp.getDataSetKey(), reparsed.getDataSetKey());
        assertEquals(dp.getStreamID(), reparsed.getStreamID());
    }
}
