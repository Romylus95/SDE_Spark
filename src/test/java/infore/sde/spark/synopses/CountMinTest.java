package infore.sde.spark.synopses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class CountMinTest {

    private static final String[] PARAMS = {"StockID", "price", "0", "0.001", "0.01", "42"};

    @Test
    void addAndEstimate() throws Exception {
        CountMin cm = new CountMin(100, PARAMS);
        ObjectMapper mapper = new ObjectMapper();

        cm.add(mapper.readTree("{\"StockID\":\"AAPL\",\"price\":\"185.0\"}"));
        cm.add(mapper.readTree("{\"StockID\":\"AAPL\",\"price\":\"188.0\"}"));
        cm.add(mapper.readTree("{\"StockID\":\"GOOG\",\"price\":\"100.0\"}"));

        long aapl = (long) cm.estimate("AAPL");
        assertThat(aapl).isEqualTo(373L); // 185 + 188

        long goog = (long) cm.estimate("GOOG");
        assertThat(goog).isEqualTo(100L);
    }

    @Test
    void estimateViaRequest() throws Exception {
        CountMin cm = new CountMin(100, PARAMS);
        ObjectMapper mapper = new ObjectMapper();

        cm.add(mapper.readTree("{\"StockID\":\"AAPL\",\"price\":\"200.0\"}"));

        Request rq = new Request("Forex", 3, 1, 100, "stream1",
                new String[]{"AAPL"}, 1);
        Estimation est = cm.estimate(rq);

        assertThat(est.getEstimation()).isEqualTo("200.0");
        assertThat(est.getUid()).isEqualTo(100);
    }

    @Test
    void countIncrementsOnAdd() throws Exception {
        CountMin cm = new CountMin(1, PARAMS);
        ObjectMapper mapper = new ObjectMapper();

        assertThat(cm.getCount()).isEqualTo(0);
        cm.add(mapper.readTree("{\"StockID\":\"X\",\"price\":\"1.0\"}"));
        assertThat(cm.getCount()).isEqualTo(1);
    }
}
