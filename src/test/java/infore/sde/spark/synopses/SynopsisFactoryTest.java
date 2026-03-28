package infore.sde.spark.synopses;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class SynopsisFactoryTest {

    @Test
    void createCountMin() throws Exception {
        String[] params = {"StockID", "price", "0", "100", "5", "0"};
        Synopsis s = SynopsisFactory.create(1, 42, params);
        assertThat(s).isInstanceOf(CountMin.class);
        assertThat(s.getSynopsisID()).isEqualTo(42);
    }

    @Test
    void createBloomFilter() throws Exception {
        String[] params = {"StockID", "price", "0", "1000", "3"};
        Synopsis s = SynopsisFactory.create(2, 10, params);
        assertThat(s).isInstanceOf(Bloomfilter.class);
    }

    @Test
    void createAMS() throws Exception {
        String[] params = {"StockID", "price", "0", "100", "5"};
        Synopsis s = SynopsisFactory.create(3, 11, params);
        assertThat(s).isInstanceOf(AMSsynopsis.class);
    }

    @Test
    void createHyperLogLog() throws Exception {
        String[] params = {"StockID", "price", "0", "0.05"};
        Synopsis s = SynopsisFactory.create(4, 12, params);
        assertThat(s).isInstanceOf(HyperLogLogSynopsis.class);
    }

    @Test
    void rejectsNullParams() {
        assertThatThrownBy(() -> SynopsisFactory.create(1, 1, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null");
    }

    @Test
    void rejectsInvalidSynopsisId() {
        assertThatThrownBy(() -> SynopsisFactory.create(0, 1, new String[]{"a"}))
                .isInstanceOf(UnsupportedSynopsisException.class);
        assertThatThrownBy(() -> SynopsisFactory.create(5, 1, new String[]{"a"}))
                .isInstanceOf(UnsupportedSynopsisException.class);
    }

    @Test
    void rejectsTooFewParams() {
        // CountMin needs 6 params
        assertThatThrownBy(() -> SynopsisFactory.create(1, 1, new String[]{"a", "b"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires at least 6");
    }
}
