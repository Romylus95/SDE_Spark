package infore.sde.spark.reduceFunctions;

import infore.sde.spark.messages.Estimation;

import java.io.Serializable;

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
