package infore.sde.spark.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SDEConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SDEConfig.class);

    private String dataTopic = "data_topic";
    private String requestTopic = "request_topic";
    private String outputTopic = "estimation_topic";
    private String kafkaBrokers = "localhost:9092";
    private String triggerInterval = "1 second";
    private String checkpointLocation = "hdfs:///sde/checkpoints";
    private Duration safetyNetTtl = Duration.ofDays(7);
    private String kafkaGroupId = "sde-spark";
    private String startingOffsets = "earliest";
    private Duration routingStateTtl = Duration.ofDays(1);
    private Duration aggregationTimeout = Duration.ofMinutes(5);
    private boolean failOnDataLoss = false;
    private String kafkaProducerAcks = "all";
    private String kafkaProducerCompression = "lz4";
    private boolean kafkaProducerIdempotence = true;
    private String kafkaSecurityProtocol = "";
    private String kafkaSaslMechanism = "";
    private String kafkaSaslJaasConfig = "";
    private String kafkaSslTruststoreLocation = "";
    private String kafkaSslTruststorePassword = "";

    public SDEConfig() {}

    /**
     * Loads config from an optional properties file first, then applies
     * CLI args on top (CLI wins over file).
     */
    public static SDEConfig fromArgs(String[] args) {
        var config = new SDEConfig();

        // First pass: look for --config flag to load a properties file
        for (int i = 0; i < args.length - 1; i++) {
            if ("--config".equals(args[i])) {
                config.loadFromFile(args[i + 1]);
                break;
            }
        }

        // Second pass: CLI args override file values
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--config" -> i++; // skip, already handled
                case "--data-topic" -> config.dataTopic = args[++i];
                case "--request-topic" -> config.requestTopic = args[++i];
                case "--output-topic" -> config.outputTopic = args[++i];
                case "--kafka-brokers" -> config.kafkaBrokers = args[++i];
                case "--trigger-interval" -> config.triggerInterval = args[++i];
                case "--checkpoint-location" -> config.checkpointLocation = args[++i];
                case "--safety-net-ttl" -> config.safetyNetTtl = Duration.parse(args[++i]);
                case "--kafka-group-id" -> config.kafkaGroupId = args[++i];
                case "--starting-offsets" -> config.startingOffsets = args[++i];
                case "--routing-state-ttl" -> config.routingStateTtl = Duration.parse(args[++i]);
                case "--aggregation-timeout" -> config.aggregationTimeout = Duration.parse(args[++i]);
                case "--fail-on-data-loss" -> config.failOnDataLoss = Boolean.parseBoolean(args[++i]);
                case "--kafka-producer-acks" -> config.kafkaProducerAcks = args[++i];
                case "--kafka-producer-compression" -> config.kafkaProducerCompression = args[++i];
                case "--kafka-producer-idempotence" -> config.kafkaProducerIdempotence = Boolean.parseBoolean(args[++i]);
                case "--kafka-security-protocol" -> config.kafkaSecurityProtocol = args[++i];
                case "--kafka-sasl-mechanism" -> config.kafkaSaslMechanism = args[++i];
                case "--kafka-sasl-jaas-config" -> config.kafkaSaslJaasConfig = args[++i];
                case "--kafka-ssl-truststore-location" -> config.kafkaSslTruststoreLocation = args[++i];
                case "--kafka-ssl-truststore-password" -> config.kafkaSslTruststorePassword = args[++i];
            }
        }
        return config;
    }

    private void loadFromFile(String path) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
            LOG.info("Loaded config from {}", path);
        } catch (IOException e) {
            LOG.warn("Failed to load config file {}: {}", path, e.getMessage());
            return;
        }
        if (props.containsKey("data.topic")) dataTopic = props.getProperty("data.topic");
        if (props.containsKey("request.topic")) requestTopic = props.getProperty("request.topic");
        if (props.containsKey("output.topic")) outputTopic = props.getProperty("output.topic");
        if (props.containsKey("kafka.brokers")) kafkaBrokers = props.getProperty("kafka.brokers");
        if (props.containsKey("trigger.interval")) triggerInterval = props.getProperty("trigger.interval");
        if (props.containsKey("checkpoint.location")) checkpointLocation = props.getProperty("checkpoint.location");
        if (props.containsKey("safety.net.ttl")) safetyNetTtl = Duration.parse(props.getProperty("safety.net.ttl"));
        if (props.containsKey("kafka.group.id")) kafkaGroupId = props.getProperty("kafka.group.id");
        if (props.containsKey("starting.offsets")) startingOffsets = props.getProperty("starting.offsets");
        if (props.containsKey("routing.state.ttl")) routingStateTtl = Duration.parse(props.getProperty("routing.state.ttl"));
        if (props.containsKey("aggregation.timeout")) aggregationTimeout = Duration.parse(props.getProperty("aggregation.timeout"));
        if (props.containsKey("fail.on.data.loss")) failOnDataLoss = Boolean.parseBoolean(props.getProperty("fail.on.data.loss"));
        if (props.containsKey("kafka.producer.acks")) kafkaProducerAcks = props.getProperty("kafka.producer.acks");
        if (props.containsKey("kafka.producer.compression")) kafkaProducerCompression = props.getProperty("kafka.producer.compression");
        if (props.containsKey("kafka.producer.idempotence")) kafkaProducerIdempotence = Boolean.parseBoolean(props.getProperty("kafka.producer.idempotence"));
        if (props.containsKey("kafka.security.protocol")) kafkaSecurityProtocol = props.getProperty("kafka.security.protocol");
        if (props.containsKey("kafka.sasl.mechanism")) kafkaSaslMechanism = props.getProperty("kafka.sasl.mechanism");
        if (props.containsKey("kafka.sasl.jaas.config")) kafkaSaslJaasConfig = props.getProperty("kafka.sasl.jaas.config");
        if (props.containsKey("kafka.ssl.truststore.location")) kafkaSslTruststoreLocation = props.getProperty("kafka.ssl.truststore.location");
        if (props.containsKey("kafka.ssl.truststore.password")) kafkaSslTruststorePassword = props.getProperty("kafka.ssl.truststore.password");
    }

    public String getDataTopic() { return dataTopic; }
    public String getRequestTopic() { return requestTopic; }
    public String getOutputTopic() { return outputTopic; }
    public String getKafkaBrokers() { return kafkaBrokers; }
    public String getTriggerInterval() { return triggerInterval; }
    public String getCheckpointLocation() { return checkpointLocation; }
    public Duration getSafetyNetTtl() { return safetyNetTtl; }
    public String getKafkaGroupId() { return kafkaGroupId; }
    public String getStartingOffsets() { return startingOffsets; }
    public Duration getRoutingStateTtl() { return routingStateTtl; }
    public Duration getAggregationTimeout() { return aggregationTimeout; }
    public boolean isFailOnDataLoss() { return failOnDataLoss; }
    public String getKafkaProducerAcks() { return kafkaProducerAcks; }
    public String getKafkaProducerCompression() { return kafkaProducerCompression; }
    public boolean isKafkaProducerIdempotence() { return kafkaProducerIdempotence; }
    public String getKafkaSecurityProtocol() { return kafkaSecurityProtocol; }
    public String getKafkaSaslMechanism() { return kafkaSaslMechanism; }
    public String getKafkaSaslJaasConfig() { return kafkaSaslJaasConfig; }
    public String getKafkaSslTruststoreLocation() { return kafkaSslTruststoreLocation; }
    public String getKafkaSslTruststorePassword() { return kafkaSslTruststorePassword; }

    /**
     * Returns Kafka security options as a map suitable for Spark's
     * readStream/writeStream .option() calls. Empty options are omitted.
     */
    public Map<String, String> getKafkaSecurityOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        if (!kafkaSecurityProtocol.isEmpty()) {
            opts.put("kafka.security.protocol", kafkaSecurityProtocol);
        }
        if (!kafkaSaslMechanism.isEmpty()) {
            opts.put("kafka.sasl.mechanism", kafkaSaslMechanism);
        }
        if (!kafkaSaslJaasConfig.isEmpty()) {
            opts.put("kafka.sasl.jaas.config", kafkaSaslJaasConfig);
        }
        if (!kafkaSslTruststoreLocation.isEmpty()) {
            opts.put("kafka.ssl.truststore.location", kafkaSslTruststoreLocation);
        }
        if (!kafkaSslTruststorePassword.isEmpty()) {
            opts.put("kafka.ssl.truststore.password", kafkaSslTruststorePassword);
        }
        return opts;
    }
}
