package infore.sde.spark.reduceFunctions;

import infore.sde.spark.messages.Estimation;

import java.util.ArrayList;
import java.util.List;

/**
 * Sums partial numeric estimations from parallel synopsis instances.
 * Used for CountMin (1), AMS (3), and HyperLogLog (4) aggregation on the PURPLE path.
 */
public class SimpleSumFunction extends ReduceFunction {

    private static final long serialVersionUID = 1L;

    private final List<Object> estimations = new ArrayList<>();

    public SimpleSumFunction(int noOfP, int count, String[] parameters, int synopsisID, int requestID) {
        super(noOfP, count, parameters, synopsisID, requestID);
    }

    @Override
    public Object reduce() {
        double sum = 0;
        for (Object entry : estimations) {
            sum += Double.parseDouble(String.valueOf(entry));
        }
        return sum;
    }

    @Override
    public boolean add(Estimation estimation) {
        estimations.add(estimation.getEstimation());
        count++;
        return count == noOfP;
    }
}
