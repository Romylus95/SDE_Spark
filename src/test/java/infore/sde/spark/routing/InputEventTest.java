package infore.sde.spark.routing;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Request;
import infore.sde.spark.processing.InputEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the union discriminator pattern (InputEvent tagging).
 * Verifies that data and request events are correctly tagged and
 * their payloads are accessible after tagging.
 */
class InputEventTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void dataEventIsTaggedCorrectly() throws Exception {
        Datapoint dp = new Datapoint("Forex", "EURUSD",
                MAPPER.readTree("{\"StockID\":\"AAPL\",\"price\":\"182.50\"}"));

        InputEvent event = InputEvent.data(dp);

        assertTrue(event.isData());
        assertFalse(event.isRequest());
        assertEquals(InputEvent.Type.DATA, event.getType());
        assertEquals("Forex", event.getDataSetKey());
        assertSame(dp, event.getDatapoint());
        assertNull(event.getRequest());
    }

    @Test
    void requestEventIsTaggedCorrectly() {
        Request rq = new Request("Forex", 1, 1, 42, "EURUSD",
                new String[]{"StockID", "price"}, 1);

        InputEvent event = InputEvent.request(rq);

        assertTrue(event.isRequest());
        assertFalse(event.isData());
        assertEquals(InputEvent.Type.REQUEST, event.getType());
        assertEquals("Forex", event.getDataSetKey());
        assertSame(rq, event.getRequest());
        assertNull(event.getDatapoint());
    }

    @Test
    void dataSetKeyPreservedForRouting() throws Exception {
        Datapoint dp = new Datapoint("Crypto", "BTCUSD",
                MAPPER.readTree("{\"coin\":\"BTC\"}"));
        Request rq = new Request("Forex", 3, 1, 10, "EURUSD",
                new String[]{"AAPL"}, 1);

        assertEquals("Crypto", InputEvent.data(dp).getDataSetKey());
        assertEquals("Forex", InputEvent.request(rq).getDataSetKey());
    }
}
