package infore.sde.spark;

import infore.sde.spark.aggregation.AggregationState;
import infore.sde.spark.aggregation.PathSplitter;
import infore.sde.spark.aggregation.ReduceAggregator;
import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.ingestion.KafkaIngestionLayer;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import infore.sde.spark.metrics.PipelineMetrics;
import infore.sde.spark.metrics.ThroughputListener;
import infore.sde.spark.output.KafkaOutputLayer;
import infore.sde.spark.processing.InputEvent;
import infore.sde.spark.processing.SynopsisProcessor;
import infore.sde.spark.processing.SynopsisProcessorState;
import infore.sde.spark.routing.DataRouter;

import infore.sde.spark.routing.RoutingState;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SDE_Spark — Synopsis Data Engine, Spark Edition.
 *
 * Main entry point. Wires together the 6-layer pipeline:
 *   Layer 1: KafkaIngestionLayer   — read raw JSON from Kafka
 *   Layer 2: RequestRouter + union — fan-out requests, merge streams
 *   Layer 3: SynopsisProcessor     — core synopsis lifecycle (in-memory state)
 *   Layer 4: PathSplitter          — green/purple path routing
 *   Layer 5: ReduceAggregator      — merge partial results (purple path)
 *   Layer 6: KafkaOutputLayer      — write estimations to Kafka
 */
public class SDESparkApp {

    private static final Logger LOG = LoggerFactory.getLogger(SDESparkApp.class);

    public static void main(String[] args) throws Exception {
        SDEConfig config = SDEConfig.fromArgs(args);

        SparkSession spark = SparkSession.builder()
                .appName("SDE_Spark")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        PipelineMetrics metrics = new PipelineMetrics(spark);

        // Metrics listener — writes per-batch throughput to CSV
        String metricsPath = System.getProperty("sde.metrics.path", "results/throughput.csv");
        ThroughputListener throughputListener = new ThroughputListener(metricsPath);
        spark.streams().addListener(throughputListener);

        LOG.info("SDE_Spark starting with config: dataTopic={}, requestTopic={}, outputTopic={}, brokers={}",
                config.getDataTopic(), config.getRequestTopic(),
                config.getOutputTopic(), config.getKafkaBrokers());

        // ──── Layer 1: Ingestion ────
        KafkaIngestionLayer ingestion = new KafkaIngestionLayer(spark, config);
        Dataset<Datapoint> dataStream = ingestion.readDataStream();
        Dataset<Request> requestStream = ingestion.readRequestStream();

        // ──── Layer 2: Routing ────
        // Union ORIGINAL (un-routed) requests with data.
        // DataRouter handles BOTH request fan-out and data fan-out in one stateful operator,
        // so the routing registration and data routing share the same partition state.
        Dataset<InputEvent> taggedData = dataStream.map(
                (MapFunction<Datapoint, InputEvent>) InputEvent::data,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> taggedRequests = requestStream.map(
                (MapFunction<Request, InputEvent>) InputEvent::request,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> combined = taggedData.union(taggedRequests);

        // ──── Layer 2b: Data + Request Routing (fan-out to keyed partitions) ────
        KeyValueGroupedDataset<String, InputEvent> groupedForRouting = combined.groupByKey(
                (MapFunction<InputEvent, String>) InputEvent::getDataSetKey,
                Encoders.STRING());

        Dataset<InputEvent> routed = groupedForRouting.flatMapGroupsWithState(
                new DataRouter(config),
                OutputMode.Append(),
                Encoders.kryo(RoutingState.class),
                Encoders.kryo(InputEvent.class),
                GroupStateTimeout.ProcessingTimeTimeout());

        // ──── Layer 3: Synopsis Processing ────
        KeyValueGroupedDataset<String, InputEvent> groupedByKey = routed.groupByKey(
                (MapFunction<InputEvent, String>) InputEvent::getDataSetKey,
                Encoders.STRING());

        Dataset<Estimation> estimations = groupedByKey.flatMapGroupsWithState(
                new SynopsisProcessor(config, metrics),
                OutputMode.Append(),
                Encoders.kryo(SynopsisProcessorState.class),
                Encoders.kryo(Estimation.class),
                GroupStateTimeout.ProcessingTimeTimeout());

        // ──── Layer 4: Path Splitting ────
        PathSplitter splitter = new PathSplitter(estimations);

        // ──── Layer 5: Aggregation (Purple path only) ────
        KeyValueGroupedDataset<Integer, Estimation> groupedByUid = splitter.getPurplePath()
                .groupByKey(
                        (MapFunction<Estimation, Integer>) Estimation::getUid,
                        Encoders.INT());

        Dataset<Estimation> aggregated = groupedByUid.flatMapGroupsWithState(
                new ReduceAggregator(config),
                OutputMode.Append(),
                Encoders.kryo(AggregationState.class),
                Encoders.kryo(Estimation.class),
                GroupStateTimeout.ProcessingTimeTimeout());

        // Merge green + purple outputs
        Dataset<Estimation> allEstimations = splitter.getGreenPath().union(aggregated);

        // ──── Layer 6: Output ────
        KafkaOutputLayer outputLayer = new KafkaOutputLayer(config);
        StreamingQuery query = outputLayer.write(allEstimations, "sde-output");

        LOG.info("SDE_Spark pipeline started. Awaiting termination...");

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received. Stopping pipeline gracefully...");
            try {
                query.stop();
                spark.stop();
                LOG.info("Pipeline stopped gracefully.");
            } catch (Exception e) {
                LOG.error("Error during graceful shutdown", e);
            }
        }));

        try {
            query.awaitTermination();
        } catch (Exception e) {
            LOG.error("Pipeline terminated with error", e);
        } finally {
            spark.stop();
        }
    }
}
