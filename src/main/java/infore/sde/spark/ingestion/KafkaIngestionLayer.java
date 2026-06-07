package infore.sde.spark.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Request;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Layer 1 — Ingestion & Parsing.
 * Consumes raw JSON from two Kafka topics (data_topic and request_topic)
 * and deserializes into typed POJOs (Datapoint and Request).
 *
 * Uses Kryo encoding for the output Datasets to support efficient serialization
 * across Spark partitions. Invalid messages are logged and filtered out.
 */
public class KafkaIngestionLayer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaIngestionLayer.class);

    private final SparkSession spark;
    private final SDEConfig config;

    public KafkaIngestionLayer(SparkSession spark, SDEConfig config) {
        this.spark = spark;
        this.config = config;
    }

    // Builds the JSON assign spec for direct partition assignment, bypassing consumer group protocol.
    // Format: {"topicName":[0,1,2,...,n-1]}
    private static String buildAssignSpec(String topic, int numPartitions) {
        StringBuilder sb = new StringBuilder("{\"").append(topic).append("\":[");
        for (int i = 0; i < numPartitions; i++) {
            if (i > 0) sb.append(",");
            sb.append(i);
        }
        sb.append("]}");
        return sb.toString();
    }

    public Dataset<Datapoint> readDataStream() {
        DataStreamReader reader = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getKafkaBrokers())
                .option("assign", buildAssignSpec(config.getDataTopic(), config.getKafkaPartitions()))
                .option("startingOffsets", config.getStartingOffsets())
                .option("failOnDataLoss", String.valueOf(config.isFailOnDataLoss()));
        if (config.getMaxOffsetsPerTrigger() > 0) {
            reader = reader.option("maxOffsetsPerTrigger", config.getMaxOffsetsPerTrigger());
        }
        config.getKafkaSecurityOptions().forEach(reader::option);
        Dataset<Row> raw = reader.load()
                .selectExpr("CAST(value AS STRING) as json");

        return raw.map((MapFunction<Row, Datapoint>) row -> {
            try {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(row.getString(0), Datapoint.class);
            } catch (Exception e) {
                LOG.warn("Failed to parse Datapoint: {}", e.getMessage());
                return null;
            }
        }, Encoders.kryo(Datapoint.class))
                .filter((FilterFunction<Datapoint>) dp -> dp != null && dp.getDataSetKey() != null);
    }

    public Dataset<Request> readRequestStream() {
        DataStreamReader reader = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getKafkaBrokers())
                .option("assign", buildAssignSpec(config.getRequestTopic(), config.getKafkaPartitions()))
                .option("startingOffsets", config.getStartingOffsets())
                .option("failOnDataLoss", String.valueOf(config.isFailOnDataLoss()));
        config.getKafkaSecurityOptions().forEach(reader::option);
        Dataset<Row> raw = reader.load()
                .selectExpr("CAST(value AS STRING) as json");

        return raw.map((MapFunction<Row, Request>) row -> {
            try {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(row.getString(0), Request.class);
            } catch (Exception e) {
                LOG.warn("Failed to parse Request: {}", e.getMessage());
                return null;
            }
        }, Encoders.kryo(Request.class))
                .filter((FilterFunction<Request>) rq -> rq != null && rq.getDataSetKey() != null);
    }
}
