package infore.sde.spark.aggregation;

import infore.sde.spark.messages.Estimation;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;

/**
 * Layer 4 — Path Splitting.
 * Routes estimations into two processing paths based on the parallelism level (noOfP):
 *   GREEN  (noOfP == 1) -> single synopsis instance, direct to output (Layer 6)
 *   PURPLE (noOfP  > 1) -> multiple parallel instances, routed to ReduceAggregator (Layer 5) first
 *
 * This is a stateless filter applied on the Estimation Dataset.
 */
public class PathSplitter {

    private final Dataset<Estimation> greenPath;
    private final Dataset<Estimation> purplePath;

    public PathSplitter(Dataset<Estimation> estimations) {
        this.greenPath = estimations.filter((FilterFunction<Estimation>) e -> e.getNoOfP() == 1);
        this.purplePath = estimations.filter((FilterFunction<Estimation>) e -> e.getNoOfP() > 1);
    }

    public Dataset<Estimation> getGreenPath() { return greenPath; }
    public Dataset<Estimation> getPurplePath() { return purplePath; }
}
