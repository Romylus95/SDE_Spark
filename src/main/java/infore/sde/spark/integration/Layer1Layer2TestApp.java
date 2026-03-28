package infore.sde.spark.integration;

import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.ingestion.KafkaIngestionLayer;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Request;
import infore.sde.spark.processing.InputEvent;
import infore.sde.spark.routing.RequestRouter;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Integration test harness for Layers 1 + 2 only.
 *
 * Reads from Kafka, parses JSON (Layer 1), routes requests (Layer 2),
 * tags + unions both streams (discriminator pattern), and prints to console.
 *
 * Usage:
 *   java --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
 *        --add-opens java.base/java.lang=ALL-UNNAMED \
 *        --add-opens java.base/java.nio=ALL-UNNAMED \
 *        --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
 *        --add-opens java.base/java.util=ALL-UNNAMED \
 *        -cp target/sde-spark-1.0.0-SNAPSHOT.jar \
 *        infore.sde.spark.integration.Layer1Layer2TestApp \
 *        --kafka-brokers localhost:9092
 */
public class Layer1Layer2TestApp {

    private static final Logger LOG = LoggerFactory.getLogger(Layer1Layer2TestApp.class);

    public static void main(String[] args) throws Exception {
        SDEConfig config = SDEConfig.fromArgs(args);

        SparkSession spark = SparkSession.builder()
                .appName("SDE_Spark_L1L2_Test")
                .master("local[*]")
                .getOrCreate();

        LOG.info("=== Layer 1+2 Test Harness ===");
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

        // ──── Layer 2: Tag + Union (discriminator pattern) ────
        Dataset<InputEvent> taggedData = dataStream.map(
                (MapFunction<Datapoint, InputEvent>) InputEvent::data,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> taggedRequests = routedRequests.map(
                (MapFunction<Request, InputEvent>) InputEvent::request,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> combined = taggedData.union(taggedRequests);

        // ──── Console output: format as readable strings ────

        // Stream 1: Show parsed datapoints (Layer 1 output)
        StreamingQuery dataQuery = dataStream
                .map((MapFunction<Datapoint, String>) dp ->
                                String.format("[L1-DATA] key=%-15s stream=%-10s values=%s",
                                        dp.getDataSetKey(), dp.getStreamID(), dp.getValues()),
                        Encoders.STRING())
                .writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .queryName("L1-datapoints")
                .start();

        // Stream 2: Show routed requests (Layer 2 output — after fan-out)
        StreamingQuery requestQuery = routedRequests
                .map((MapFunction<Request, String>) rq ->
                                String.format("[L2-REQ]  key=%-30s reqID=%d synID=%d uid=%d noOfP=%d param=%s",
                                        rq.getDataSetKey(), rq.getRequestID(),
                                        rq.getSynopsisID(), rq.getUid(), rq.getNoOfP(),
                                        Arrays.toString(rq.getParam())),
                        Encoders.STRING())
                .writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .queryName("L2-routed-requests")
                .start();

        // Stream 3: Show combined tagged events (what Layer 3 groupByKey receives)
        StreamingQuery combinedQuery = combined
                .map((MapFunction<InputEvent, String>) event -> {
                    if (event.isData()) {
                        return String.format("[COMBINED-DATA]    key=%-30s streamID=%s",
                                event.getDataSetKey(), event.getDatapoint().getStreamID());
                    } else {
                        Request rq = event.getRequest();
                        return String.format("[COMBINED-REQUEST] key=%-30s reqID=%d synID=%d uid=%d noOfP=%d",
                                event.getDataSetKey(), rq.getRequestID(),
                                rq.getSynopsisID(), rq.getUid(), rq.getNoOfP());
                    }
                }, Encoders.STRING())
                .writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .queryName("L2-combined-stream")
                .start();

        LOG.info("=== Streams started. Send messages to Kafka to see output. ===");
        LOG.info("=== Press Ctrl+C to stop. ===");

        spark.streams().awaitAnyTermination();
    }
}
