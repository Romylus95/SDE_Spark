package infore.sde.spark.integration;

import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.ingestion.KafkaIngestionLayer;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import infore.sde.spark.output.KafkaOutputLayer;
import infore.sde.spark.processing.CombinedProcessor;
import infore.sde.spark.processing.CombinedState;
import infore.sde.spark.processing.InputEvent;

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

        // ──── Layers 2+3: Combined routing + synopsis lifecycle ────
        Dataset<InputEvent> taggedData = dataStream.map(
                (MapFunction<Datapoint, InputEvent>) InputEvent::data,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> taggedRequests = requestStream.map(
                (MapFunction<Request, InputEvent>) InputEvent::request,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> combined = taggedData.union(taggedRequests);

        KeyValueGroupedDataset<String, InputEvent> groupedByBaseKey = combined.groupByKey(
                (MapFunction<InputEvent, String>) InputEvent::getDataSetKey,
                Encoders.STRING());

        Dataset<Estimation> allEstimations = groupedByBaseKey.flatMapGroupsWithState(
                new CombinedProcessor(config),
                OutputMode.Append(),
                Encoders.kryo(CombinedState.class),
                Encoders.kryo(Estimation.class),
                GroupStateTimeout.ProcessingTimeTimeout());

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
