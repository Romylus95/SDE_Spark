package infore.sde.spark.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Test Kafka producer for Layer 1+2 validation.
 *
 * Sends sample data events and request commands to Kafka topics,
 * matching the exact JSON wire format of the Flink SDE.
 *
 * Usage:
 *   java -cp sde-spark-1.0.0-SNAPSHOT.jar \
 *     infore.sde.spark.integration.TestKafkaProducer \
 *     localhost:9092
 */
public class TestKafkaProducer {

    private static final Logger LOG = LoggerFactory.getLogger(TestKafkaProducer.class);

    public static void main(String[] args) throws Exception {
        String brokers = args.length > 0 ? args[0] : "localhost:9092";
        String dataTopic = args.length > 1 ? args[1] : "data_topic";
        String requestTopic = args.length > 2 ? args[2] : "request_topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            LOG.info("=== Sending test messages to {} ===", brokers);

            // ── Step 1: Send ADD request (noOfP=1, single partition) ──
            String addSingle = """
                    {"dataSetkey":"Forex","requestID":1,"synopsisID":1,"uid":10,"streamID":"EURUSD","param":["StockID","price","Queryable","0.01","0.99","42"],"noOfP":1}""";
            send(producer, requestTopic, "Forex", addSingle);
            LOG.info("Sent ADD request uid=10 noOfP=1 (single partition)");

            // ── Step 2: Send ADD request (noOfP=3, triggers fan-out) ──
            String addMulti = """
                    {"dataSetkey":"Forex","requestID":1,"synopsisID":4,"uid":42,"streamID":"EURUSD","param":["StockID","price","Queryable","0.05"],"noOfP":3}""";
            send(producer, requestTopic, "Forex", addMulti);
            LOG.info("Sent ADD request uid=42 noOfP=3 (should fan-out to 3 copies)");

            // ── Step 3: Send data events ──
            String[] stocks = {"AAPL", "GOOG", "MSFT", "TSLA", "AAPL"};
            String[] prices = {"182.50", "141.20", "378.90", "248.50", "183.00"};

            for (int i = 0; i < stocks.length; i++) {
                String data = String.format(
                        "{\"dataSetkey\":\"Forex\",\"streamID\":\"EURUSD\",\"values\":{\"StockID\":\"%s\",\"price\":\"%s\"}}",
                        stocks[i], prices[i]);
                send(producer, dataTopic, "Forex", data);
                LOG.info("Sent data: StockID={} price={}", stocks[i], prices[i]);
                Thread.sleep(500);
            }

            // ── Step 4: Send ESTIMATE request ──
            String estimate = """
                    {"dataSetkey":"Forex","requestID":3,"synopsisID":1,"uid":10,"streamID":"EURUSD","param":["AAPL"],"noOfP":1}""";
            send(producer, requestTopic, "Forex", estimate);
            LOG.info("Sent ESTIMATE request uid=10");

            // ── Step 5: Send DELETE request ──
            String delete = """
                    {"dataSetkey":"Forex","requestID":2,"synopsisID":1,"uid":10,"streamID":"EURUSD","param":[],"noOfP":1}""";
            send(producer, requestTopic, "Forex", delete);
            LOG.info("Sent DELETE request uid=10");

            producer.flush();
            LOG.info("=== All test messages sent. ===");
        }
    }

    private static void send(KafkaProducer<String, String> producer,
                             String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }
}
