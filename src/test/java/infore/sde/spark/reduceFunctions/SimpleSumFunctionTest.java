package infore.sde.spark.reduceFunctions;

import infore.sde.spark.messages.Estimation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class SimpleSumFunctionTest {

    @Test
    void sumsTwoPartials() {
        SimpleSumFunction fn = new SimpleSumFunction(2, 0, new String[]{"AAPL"}, 1, 3);

        Estimation partial1 = new Estimation(42, "key1", 3, 1, "Forex", "185.0", new String[]{"AAPL"}, 2);
        Estimation partial2 = new Estimation(42, "key2", 3, 1, "Forex", "373.0", new String[]{"AAPL"}, 2);

        assertThat(fn.add(partial1)).isFalse();
        assertThat(fn.add(partial2)).isTrue();

        double result = (double) fn.reduce();
        assertThat(result).isEqualTo(558.0); // 185 + 373
    }

    @Test
    void sumsThreePartials() {
        SimpleSumFunction fn = new SimpleSumFunction(3, 0, new String[]{"X"}, 1, 3);

        fn.add(new Estimation(1, "k", 3, 1, "K", "10.0", new String[]{"X"}, 3));
        fn.add(new Estimation(1, "k", 3, 1, "K", "20.0", new String[]{"X"}, 3));
        assertThat(fn.add(new Estimation(1, "k", 3, 1, "K", "30.0", new String[]{"X"}, 3))).isTrue();

        assertThat((double) fn.reduce()).isEqualTo(60.0);
    }

    @Test
    void singlePartialReturnsImmediately() {
        SimpleSumFunction fn = new SimpleSumFunction(1, 0, new String[]{"X"}, 1, 3);
        assertThat(fn.add(new Estimation(1, "k", 3, 1, "K", "42.0", new String[]{"X"}, 1))).isTrue();
        assertThat((double) fn.reduce()).isEqualTo(42.0);
    }
}
