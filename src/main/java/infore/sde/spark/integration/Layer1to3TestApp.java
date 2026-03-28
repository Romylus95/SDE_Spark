package infore.sde.spark.integration;

import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.ingestion.KafkaIngestionLayer;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import infore.sde.spark.processing.InputEvent;
import infore.sde.spark.processing.SynopsisProcessor;
import infore.sde.spark.processing.SynopsisProcessorState;
import infore.sde.spark.routing.RequestRouter;
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
 * Integration test harness for Layers 1 + 2 + 3.
 *
 * Reads from Kafka, parses JSON (Layer 1), routes requests (Layer 2),
 * processes synopses via flatMapGroupsWithState (Layer 3),
 * and prints Estimation results to console.
 *
 * Usage:
 *   java --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
 *        --add-opens java.base/java.lang=ALL-UNNAMED \
 *        --add-opens java.base/java.nio=ALL-UNNAMED \
 *        --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
 *        --add-opens java.base/java.util=ALL-UNNAMED \
 *        -cp target/sde-spark-1.0.0-SNAPSHOT.jar \
 *        infore.sde.spark.integration.Layer1to3TestApp \
 *        --kafka-brokers localhost:9092
 */
public class Layer1to3TestApp {

    private static final Logger LOG = LoggerFactory.getLogger(Layer1to3TestApp.class);

    public static void main(String[] args) throws Exception {
        SDEConfig config = SDEConfig.fromArgs(args);

        SparkSession spark = SparkSession.builder()
                .appName("SDE_Spark_L1L2L3_Test")
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.streaming.checkpointLocation",
                        System.getProperty("java.io.tmpdir") + "/sde-spark-l3-test")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        LOG.info("=== Layer 1+2+3 Test Harness ===");
        LOG.info("Kafka brokers: {}", config.getKafkaBrokers());
        LOG.info("Data topic:    {}", config.getDataTopic());
        LOG.info("Request topic: {}", config.getRequestTopic());

        // ──── Layer 1: Ingestion ────
        KafkaIngestionLayer ingestion = new KafkaIngestionLayer(spark, config);
        Dataset<Datapoint> dataStream = ingestion.readDataStream();
        Dataset<Request> requestStream = ingestion.readRequestStream();

        // ──── Layer 2: Request Routing (fan-out) ────
        Dataset<Request> routedRequests = requestStream.flatMap(
                new RequestRouter(), Encoders.kryo(Request.class));

        // Tag and union both streams (discriminator pattern)
        Dataset<InputEvent> taggedData = dataStream.map(
                (MapFunction<Datapoint, InputEvent>) InputEvent::data,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> taggedRequests = routedRequests.map(
                (MapFunction<Request, InputEvent>) InputEvent::request,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> combined = taggedData.union(taggedRequests);

        // ──── Layer 3: Synopsis Processing ────
        KeyValueGroupedDataset<String, InputEvent> groupedByKey = combined.groupByKey(
                (MapFunction<InputEvent, String>) InputEvent::getDataSetKey,
                Encoders.STRING());

        Dataset<Estimation> estimations = groupedByKey.flatMapGroupsWithState(
                new SynopsisProcessor(config),
                OutputMode.Append(),
                Encoders.kryo(SynopsisProcessorState.class),
                Encoders.kryo(Estimation.class),
                GroupStateTimeout.ProcessingTimeTimeout());

        // ──── Console output: format estimations as readable strings ────
        StreamingQuery estimationQuery = estimations
                .map((MapFunction<Estimation, String>) est ->
                                String.format("[L3-ESTIMATION] uid=%-5d reqID=%-5d synID=%d noOfP=%d key=%-30s estimation=%s param=%s",
                                        est.getUid(), est.getRequestID(),
                                        est.getSynopsisID(), est.getNoOfP(),
                                        est.getKey(), est.getEstimation(),
                                        java.util.Arrays.toString(est.getParam())),
                        Encoders.STRING())
                .writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .queryName("L3-estimations")
                .start();

        LOG.info("=== Streams started. Send messages to Kafka to see output. ===");
        LOG.info("=== Press Ctrl+C to stop. ===");

        spark.streams().awaitAnyTermination();
    }
}
