package infore.sde.spark.reduceFunctions;

import infore.sde.spark.messages.Estimation;

import java.io.Serializable;

/**
 * Abstract base for PURPLE path aggregation functions.
 * Collects partial estimation results from N parallel synopsis partitions
 * and produces a single merged result when all noOfP partials have arrived.
 *
 * Implementations: SimpleSumFunction (numeric sum), SimpleORFunction (boolean OR).
 */
public abstract class ReduceFunction implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final int noOfP;
    protected int count;
    protected final String[] parameters;
    protected final int synopsisID;
    protected final int requestID;

    protected ReduceFunction(int noOfP, int count, String[] parameters, int synopsisID, int requestID) {
        this.noOfP = noOfP;
        this.count = count;
        this.parameters = parameters;
        this.synopsisID = synopsisID;
        this.requestID = requestID;
    }

    public abstract Object reduce();

    public abstract boolean add(Estimation estimation);

    public int getNoOfP() { return noOfP; }
    public int getCount() { return count; }
}
