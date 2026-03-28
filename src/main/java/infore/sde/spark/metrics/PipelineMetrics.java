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

    private final LongAccumulator datapointsProcessed;
    private final LongAccumulator requestsProcessed;
    private final LongAccumulator synopsesCreated;
    private final LongAccumulator synopsesDeleted;
    private final LongAccumulator estimationsEmitted;
    private final LongAccumulator aggregationsCompleted;
    private final LongAccumulator parseErrors;
    private final LongAccumulator routingFanOuts;
    private final LongAccumulator stateTimeouts;

    public PipelineMetrics(SparkSession spark) {
        this.datapointsProcessed = spark.sparkContext().longAccumulator("sde.datapoints.processed");
        this.requestsProcessed = spark.sparkContext().longAccumulator("sde.requests.processed");
        this.synopsesCreated = spark.sparkContext().longAccumulator("sde.synopses.created");
        this.synopsesDeleted = spark.sparkContext().longAccumulator("sde.synopses.deleted");
        this.estimationsEmitted = spark.sparkContext().longAccumulator("sde.estimations.emitted");
        this.aggregationsCompleted = spark.sparkContext().longAccumulator("sde.aggregations.completed");
        this.parseErrors = spark.sparkContext().longAccumulator("sde.parse.errors");
        this.routingFanOuts = spark.sparkContext().longAccumulator("sde.routing.fanouts");
        this.stateTimeouts = spark.sparkContext().longAccumulator("sde.state.timeouts");
    }

    public void incDatapointsProcessed(long n) { datapointsProcessed.add(n); }
    public void incRequestsProcessed() { requestsProcessed.add(1); }
    public void incSynopsesCreated() { synopsesCreated.add(1); }
    public void incSynopsesDeleted() { synopsesDeleted.add(1); }
    public void incEstimationsEmitted() { estimationsEmitted.add(1); }
    public void incAggregationsCompleted() { aggregationsCompleted.add(1); }
    public void incParseErrors() { parseErrors.add(1); }
    public void incRoutingFanOuts() { routingFanOuts.add(1); }
    public void incStateTimeouts() { stateTimeouts.add(1); }

    public long getDatapointsProcessed() { return datapointsProcessed.value(); }
    public long getRequestsProcessed() { return requestsProcessed.value(); }
    public long getSynopsesCreated() { return synopsesCreated.value(); }
    public long getEstimationsEmitted() { return estimationsEmitted.value(); }
    public long getAggregationsCompleted() { return aggregationsCompleted.value(); }
    public long getParseErrors() { return parseErrors.value(); }
}
