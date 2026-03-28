package infore.sde.spark.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.messages.Estimation;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Layer 6 — Output.
 * Serializes Estimation results to JSON and writes to Kafka output topic.
 *
 * Replaces: kafkaProducerEstimation + addSink() from Flink Run.java
 */
public class KafkaOutputLayer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOutputLayer.class);

    private final SDEConfig config;

    public KafkaOutputLayer(SDEConfig config) {
        this.config = config;
    }

    public StreamingQuery write(Dataset<Estimation> estimations, String queryName)
            throws Exception {

        Dataset<String> jsonOutput = estimations.map(
                (MapFunction<Estimation, String>) estimation -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        return mapper.writeValueAsString(estimation);
                    } catch (Exception e) {
                        LOG.warn("Failed to serialize Estimation: {}", e.getMessage());
                        return null;
                    }
                }, Encoders.STRING())
                .filter((FilterFunction<String>) s -> s != null);

        DataStreamWriter<org.apache.spark.sql.Row> writer = jsonOutput
                .toDF("value")
                .selectExpr("value AS key", "value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.getKafkaBrokers())
                .option("topic", config.getOutputTopic())
                .option("kafka.acks", config.getKafkaProducerAcks())
                .option("kafka.compression.type", config.getKafkaProducerCompression())
                .option("kafka.enable.idempotence", String.valueOf(config.isKafkaProducerIdempotence()))
                .option("checkpointLocation", config.getCheckpointLocation() + "/" + queryName)
                .trigger(Trigger.ProcessingTime(config.getTriggerInterval()))
                .queryName(queryName);
        config.getKafkaSecurityOptions().forEach(writer::option);
        return writer.start();
    }
}
