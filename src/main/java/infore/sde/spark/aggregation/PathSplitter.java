package infore.sde.spark.aggregation;

import infore.sde.spark.messages.Estimation;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;

/**
 * Layer 4 — Path Splitting.
 * Routes estimations based on noOfP:
 *   GREEN  (noOfP == 1) -> direct to output
 *   PURPLE (noOfP  > 1) -> to ReduceAggregator first
 *
 * Replaces: SplitStream / OutputSelector from Flink Run.java
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
