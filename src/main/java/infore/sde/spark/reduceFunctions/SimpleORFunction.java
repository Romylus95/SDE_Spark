package infore.sde.spark.reduceFunctions;

import infore.sde.spark.messages.Estimation;

import java.util.ArrayList;
import java.util.List;

/**
 * ORs partial boolean estimations from parallel synopsis instances.
 * Used for BloomFilter (2) aggregation on the PURPLE path:
 * if any partition reports "present", the final result is true.
 */
public class SimpleORFunction extends ReduceFunction {

    private static final long serialVersionUID = 1L;

    private final List<Object> estimations = new ArrayList<>();

    public SimpleORFunction(int noOfP, int count, String[] parameters, int synopsisID, int requestID) {
        super(noOfP, count, parameters, synopsisID, requestID);
    }

    @Override
    public Object reduce() {
        boolean result = false;
        for (Object entry : estimations) {
            result = result || (boolean) entry;
        }
        return result;
    }

    @Override
    public boolean add(Estimation estimation) {
        estimations.add(estimation.getEstimation());
        count++;
        return count == noOfP;
    }
}
