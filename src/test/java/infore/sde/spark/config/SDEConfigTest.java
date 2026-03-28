package infore.sde.spark.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileWriter;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

class SDEConfigTest {

    @Test
    void defaultValues() {
        SDEConfig config = new SDEConfig();
        assertThat(config.getDataTopic()).isEqualTo("data_topic");
        assertThat(config.getRequestTopic()).isEqualTo("request_topic");
        assertThat(config.getOutputTopic()).isEqualTo("estimation_topic");
        assertThat(config.getKafkaBrokers()).isEqualTo("localhost:9092");
        assertThat(config.getKafkaGroupId()).isEqualTo("sde-spark");
        assertThat(config.getStartingOffsets()).isEqualTo("earliest");
        assertThat(config.getRoutingStateTtl()).isEqualTo(Duration.ofDays(1));
        assertThat(config.getAggregationTimeout()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.isFailOnDataLoss()).isFalse();
        assertThat(config.getKafkaProducerAcks()).isEqualTo("all");
        assertThat(config.getKafkaProducerCompression()).isEqualTo("lz4");
        assertThat(config.isKafkaProducerIdempotence()).isTrue();
    }

    @Test
    void fromArgsOverridesDefaults() {
        String[] args = {
                "--data-topic", "my-data",
                "--kafka-brokers", "broker1:9092,broker2:9092",
                "--kafka-group-id", "my-group",
                "--aggregation-timeout", "PT10M",
                "--routing-state-ttl", "PT12H"
        };
        SDEConfig config = SDEConfig.fromArgs(args);

        assertThat(config.getDataTopic()).isEqualTo("my-data");
        assertThat(config.getKafkaBrokers()).isEqualTo("broker1:9092,broker2:9092");
        assertThat(config.getKafkaGroupId()).isEqualTo("my-group");
        assertThat(config.getAggregationTimeout()).isEqualTo(Duration.ofMinutes(10));
        assertThat(config.getRoutingStateTtl()).isEqualTo(Duration.ofHours(12));
        // Unchanged defaults
        assertThat(config.getRequestTopic()).isEqualTo("request_topic");
    }

    @Test
    void kafkaSecurityOptionsEmptyByDefault() {
        SDEConfig config = new SDEConfig();
        assertThat(config.getKafkaSecurityOptions()).isEmpty();
    }

    @Test
    void kafkaSecurityOptionsPopulated() {
        String[] args = {
                "--kafka-security-protocol", "SASL_SSL",
                "--kafka-sasl-mechanism", "PLAIN",
                "--kafka-ssl-truststore-location", "/path/to/truststore"
        };
        SDEConfig config = SDEConfig.fromArgs(args);

        assertThat(config.getKafkaSecurityOptions())
                .containsEntry("kafka.security.protocol", "SASL_SSL")
                .containsEntry("kafka.sasl.mechanism", "PLAIN")
                .containsEntry("kafka.ssl.truststore.location", "/path/to/truststore")
                .doesNotContainKey("kafka.ssl.truststore.password")
                .doesNotContainKey("kafka.sasl.jaas.config");
    }

    @Test
    void failOnDataLossAndProducerTuningFromArgs() {
        String[] args = {
                "--fail-on-data-loss", "true",
                "--kafka-producer-acks", "1",
                "--kafka-producer-compression", "snappy",
                "--kafka-producer-idempotence", "false"
        };
        SDEConfig config = SDEConfig.fromArgs(args);

        assertThat(config.isFailOnDataLoss()).isTrue();
        assertThat(config.getKafkaProducerAcks()).isEqualTo("1");
        assertThat(config.getKafkaProducerCompression()).isEqualTo("snappy");
        assertThat(config.isKafkaProducerIdempotence()).isFalse();
    }

    @Test
    void loadFromConfigFile(@TempDir Path tempDir) throws Exception {
        Path configFile = tempDir.resolve("test.properties");
        try (FileWriter w = new FileWriter(configFile.toFile())) {
            w.write("data.topic=file-data\n");
            w.write("kafka.brokers=broker-from-file:9092\n");
            w.write("fail.on.data.loss=true\n");
            w.write("kafka.producer.compression=gzip\n");
        }

        // Config file values
        SDEConfig config = SDEConfig.fromArgs(new String[]{
                "--config", configFile.toString()
        });
        assertThat(config.getDataTopic()).isEqualTo("file-data");
        assertThat(config.getKafkaBrokers()).isEqualTo("broker-from-file:9092");
        assertThat(config.isFailOnDataLoss()).isTrue();
        assertThat(config.getKafkaProducerCompression()).isEqualTo("gzip");
        // Default for values not in file
        assertThat(config.getRequestTopic()).isEqualTo("request_topic");
    }

    @Test
    void cliArgsOverrideConfigFile(@TempDir Path tempDir) throws Exception {
        Path configFile = tempDir.resolve("test.properties");
        try (FileWriter w = new FileWriter(configFile.toFile())) {
            w.write("data.topic=from-file\n");
            w.write("kafka.brokers=file-broker:9092\n");
        }

        SDEConfig config = SDEConfig.fromArgs(new String[]{
                "--config", configFile.toString(),
                "--data-topic", "from-cli"
        });
        // CLI wins
        assertThat(config.getDataTopic()).isEqualTo("from-cli");
        // File value retained where no CLI override
        assertThat(config.getKafkaBrokers()).isEqualTo("file-broker:9092");
    }
}
