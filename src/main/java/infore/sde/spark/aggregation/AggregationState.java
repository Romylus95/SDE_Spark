package infore.sde.spark.aggregation;

import infore.sde.spark.messages.Estimation;
import infore.sde.spark.reduceFunctions.ReduceFunction;

import java.io.Serializable;

/**
 * State for the ReduceAggregator.
 * Buffers partial results until all noOfP partials have arrived.
 */
public class AggregationState implements Serializable {

    private static final long serialVersionUID = 1L;

    private ReduceFunction reducer;
    private Estimation template;

    public ReduceFunction getReducer() { return reducer; }
    public void setReducer(ReduceFunction reducer) { this.reducer = reducer; }

    public Estimation getTemplate() { return template; }
    public void setTemplate(Estimation template) { this.template = template; }
}
