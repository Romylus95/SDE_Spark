package infore.sde.spark.aggregation;

import infore.sde.spark.messages.Estimation;
import infore.sde.spark.reduceFunctions.SimpleORFunction;
import infore.sde.spark.reduceFunctions.SimpleSumFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReduceFunctionTest {

    @Test
    void simpleSumCollectsAndReduces() {
        SimpleSumFunction sum = new SimpleSumFunction(3, 0, new String[]{}, 1, 3);

        assertFalse(sum.add(makeEstimation("10.0")));
        assertFalse(sum.add(makeEstimation("20.0")));
        assertTrue(sum.add(makeEstimation("12.5")));

        assertEquals(42.5, (double) sum.reduce(), 0.001);
    }

    @Test
    void simpleORReducesCorrectly() {
        SimpleORFunction orFn = new SimpleORFunction(3, 0, new String[]{}, 2, 3);

        assertFalse(orFn.add(makeEstimation(false)));
        assertFalse(orFn.add(makeEstimation(true)));
        assertTrue(orFn.add(makeEstimation(false)));

        assertEquals(true, orFn.reduce());
    }

    @Test
    void simpleORAllFalse() {
        SimpleORFunction orFn = new SimpleORFunction(2, 0, new String[]{}, 2, 3);

        assertFalse(orFn.add(makeEstimation(false)));
        assertTrue(orFn.add(makeEstimation(false)));

        assertEquals(false, orFn.reduce());
    }

    private Estimation makeEstimation(Object value) {
        return new Estimation(1, "key", 3, 1, "ds", value, new String[]{}, 1);
    }
}
