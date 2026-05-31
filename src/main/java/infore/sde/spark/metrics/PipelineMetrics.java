package infore.sde.spark.metrics;

import org.apache.spark.util.LongAccumulator;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Pipeline-wide metrics via Spark accumulators.
 * Visible in Spark UI under "Accumulators" on the SQL/Streaming tab.
 *
 * Usage: pass the singleton to processors; call increment methods from
 * within flatMapGroupsWithState. The Spark driver aggregates totals.
 */
public class PipelineMetrics implements Serializable {

    private static final long serialVersionUID = 1L;

    private final LongAccumulator synopsesCreated;
    private final LongAccumulator synopsesDeleted;
    private final LongAccumulator estimationsEmitted;
    private final LongAccumulator stateTimeouts;

    public PipelineMetrics(SparkSession spark) {
        this.synopsesCreated = spark.sparkContext().longAccumulator("sde.synopses.created");
        this.synopsesDeleted = spark.sparkContext().longAccumulator("sde.synopses.deleted");
        this.estimationsEmitted = spark.sparkContext().longAccumulator("sde.estimations.emitted");
        this.stateTimeouts = spark.sparkContext().longAccumulator("sde.state.timeouts");
    }

    public void incSynopsesCreated() { synopsesCreated.add(1); }
    public void incSynopsesDeleted() { synopsesDeleted.add(1); }
    public void incEstimationsEmitted() { estimationsEmitted.add(1); }
    public void incStateTimeouts() { stateTimeouts.add(1); }
}
