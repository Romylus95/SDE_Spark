package infore.sde.spark.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Request;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class InputEventTest {

    @Test
    void dataEvent() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Datapoint dp = mapper.readValue(
                "{\"dataSetkey\":\"Forex\",\"streamID\":\"EURUSD\",\"values\":{\"StockID\":\"AAPL\",\"price\":\"100\"}}",
                Datapoint.class);

        InputEvent event = InputEvent.data(dp);
        assertThat(event.isData()).isTrue();
        assertThat(event.isRequest()).isFalse();
        assertThat(event.getDataSetKey()).isEqualTo("Forex");
        assertThat(event.getDatapoint()).isSameAs(dp);
        assertThat(event.getRequest()).isNull();
    }

    @Test
    void requestEvent() {
        Request rq = new Request("Forex", 1, 1, 42, "stream1",
                new String[]{"StockID", "price", "0", "0.001", "0.01", "1"}, 1);

        InputEvent event = InputEvent.request(rq);
        assertThat(event.isRequest()).isTrue();
        assertThat(event.isData()).isFalse();
        assertThat(event.getDataSetKey()).isEqualTo("Forex");
        assertThat(event.getRequest()).isSameAs(rq);
        assertThat(event.getDatapoint()).isNull();
    }
}
