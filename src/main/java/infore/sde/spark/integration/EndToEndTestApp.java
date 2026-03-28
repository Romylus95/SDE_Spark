package infore.sde.spark.integration;

import infore.sde.spark.aggregation.AggregationState;
import infore.sde.spark.aggregation.PathSplitter;
import infore.sde.spark.aggregation.ReduceAggregator;
import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.ingestion.KafkaIngestionLayer;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
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
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end integration test: all 6 layers.
 *
 * Identical to SDESparkApp but runs locally and also mirrors output to console
 * so we can see results without needing a separate Kafka consumer.
 *
 * Writes to Kafka estimation_topic (Layer 6) AND prints to console.
 *
 * Usage:
 *   java --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
 *        --add-opens java.base/java.lang=ALL-UNNAMED \
 *        --add-opens java.base/java.nio=ALL-UNNAMED \
 *        --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
 *        --add-opens java.base/java.util=ALL-UNNAMED \
 *        -cp target/sde-spark-1.0.0-SNAPSHOT.jar \
 *        infore.sde.spark.integration.EndToEndTestApp \
 *        --kafka-brokers localhost:9092
 */
public class EndToEndTestApp {

    private static final Logger LOG = LoggerFactory.getLogger(EndToEndTestApp.class);

    public static void main(String[] args) throws Exception {
        // Override checkpoint to local temp for testing (default is hdfs://)
        String checkpointBase = System.getProperty("java.io.tmpdir") + "/sde-spark-e2e-test";
        String[] fullArgs = java.util.stream.Stream.concat(
                java.util.Arrays.stream(args),
                java.util.Arrays.stream(new String[]{"--checkpoint-location", checkpointBase})
        ).toArray(String[]::new);
        SDEConfig config = SDEConfig.fromArgs(fullArgs);

        SparkSession spark = SparkSession.builder()
                .appName("SDE_Spark_E2E_Test")
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.streaming.checkpointLocation", checkpointBase)
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        LOG.info("=== End-to-End Test (All 6 Layers) ===");
        LOG.info("Kafka brokers:  {}", config.getKafkaBrokers());
        LOG.info("Data topic:     {}", config.getDataTopic());
        LOG.info("Request topic:  {}", config.getRequestTopic());
        LOG.info("Output topic:   {}", config.getOutputTopic());

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
        // Re-group by potentially new keys (e.g., "Forex_2_KEYED_0")
        KeyValueGroupedDataset<String, InputEvent> groupedByKey = routed.groupByKey(
                (MapFunction<InputEvent, String>) InputEvent::getDataSetKey,
                Encoders.STRING());

        Dataset<Estimation> estimations = groupedByKey.flatMapGroupsWithState(
                new SynopsisProcessor(config),
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

        // ──── Layer 6: Output to Kafka ────
        KafkaOutputLayer outputLayer = new KafkaOutputLayer(config);
        StreamingQuery kafkaQuery = outputLayer.write(allEstimations, "sde-e2e-output");

        // ──── Console output (for test visibility) ────
        StreamingQuery consoleQuery = allEstimations
                .map((MapFunction<Estimation, String>) est -> {
                    String path = est.getNoOfP() == 1 ? "GREEN" : "PURPLE";
                    return String.format("[%s] uid=%-5d reqID=%-5d synID=%d noOfP=%d key=%-30s estimation=%s",
                            path, est.getUid(), est.getRequestID(),
                            est.getSynopsisID(), est.getNoOfP(),
                            est.getKey(), est.getEstimation());
                }, Encoders.STRING())
                .writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .queryName("e2e-console")
                .start();

        LOG.info("=== All 6 layers started. Send messages to Kafka. ===");
        LOG.info("=== Press Ctrl+C to stop. ===");

        spark.streams().awaitAnyTermination();
    }
}
