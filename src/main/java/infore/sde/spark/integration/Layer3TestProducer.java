package infore.sde.spark.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Test Kafka producer for Layer 3 (SynopsisProcessor) validation.
 *
 * Sends a carefully sequenced set of messages to exercise:
 *   1. ADD    — register a CountMin synopsis (synID=1)
 *   2. DATA   — feed stock data into it
 *   3. ESTIMATE — query the CountMin for "AAPL" count
 *   4. ADD    — register a BloomFilter synopsis (synID=2)
 *   5. DATA   — feed more data (both synopses receive it)
 *   6. ESTIMATE — query the BloomFilter for "AAPL" membership
 *   7. ADD    — register a HyperLogLog synopsis (synID=4)
 *   8. DATA   — feed data with repeated and unique items
 *   9. ESTIMATE — query HyperLogLog for cardinality
 *  10. DELETE  — remove the CountMin
 *  11. ESTIMATE — try to estimate on deleted CountMin (should produce nothing)
 *  12. ESTIMATE — BloomFilter still works after CountMin deletion
 *
 * Usage:
 *   java -cp sde-spark-1.0.0-SNAPSHOT.jar \
 *     infore.sde.spark.integration.Layer3TestProducer \
 *     localhost:9092
 */
public class Layer3TestProducer {

    private static final Logger LOG = LoggerFactory.getLogger(Layer3TestProducer.class);

    public static void main(String[] args) throws Exception {
        String brokers = args.length > 0 ? args[0] : "localhost:9092";
        String dataTopic = args.length > 1 ? args[1] : "data_topic";
        String requestTopic = args.length > 2 ? args[2] : "request_topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            LOG.info("=== Layer 3 Test: Sending sequenced messages to {} ===", brokers);

            // ────────────────────────────────────────────────────────────────
            // STEP 1: ADD a CountMin synopsis (synID=1, uid=10, noOfP=1)
            //   params: [keyField, valueField, operationMode, epsilon, delta, seed]
            // ────────────────────────────────────────────────────────────────
            send(producer, requestTopic, "Forex",
                    """
                    {"dataSetkey":"Forex","requestID":1,"synopsisID":1,"uid":10,"streamID":"EURUSD","param":["StockID","price","Queryable","0.01","0.99","42"],"noOfP":1}""");
            LOG.info("STEP 1: ADD CountMin uid=10 synID=1");
            producer.flush();
            Thread.sleep(3000);  // Wait for Spark to process the ADD

            // ────────────────────────────────────────────────────────────────
            // STEP 2: Send data events (AAPL x3, GOOG x1, MSFT x1)
            // ────────────────────────────────────────────────────────────────
            String[][] stocks = {
                    {"AAPL", "182.50"},
                    {"AAPL", "183.00"},
                    {"GOOG", "141.20"},
                    {"AAPL", "184.00"},
                    {"MSFT", "378.90"}
            };

            for (String[] stock : stocks) {
                String data = String.format(
                        "{\"dataSetkey\":\"Forex\",\"streamID\":\"EURUSD\",\"values\":{\"StockID\":\"%s\",\"price\":\"%s\"}}",
                        stock[0], stock[1]);
                send(producer, dataTopic, "Forex", data);
                LOG.info("STEP 2: DATA StockID={} price={}", stock[0], stock[1]);
                Thread.sleep(200);
            }
            producer.flush();
            Thread.sleep(3000);  // Wait for data to be processed

            // ────────────────────────────────────────────────────────────────
            // STEP 3: ESTIMATE on CountMin — query "AAPL" count
            //   Expected: ~3 (AAPL appeared 3 times)
            // ────────────────────────────────────────────────────────────────
            send(producer, requestTopic, "Forex",
                    """
                    {"dataSetkey":"Forex","requestID":3,"synopsisID":1,"uid":10,"streamID":"EURUSD","param":["AAPL"],"noOfP":1}""");
            LOG.info("STEP 3: ESTIMATE CountMin uid=10 query='AAPL' (expect ~3)");
            producer.flush();
            Thread.sleep(3000);

            // ────────────────────────────────────────────────────────────────
            // STEP 4: ADD a BloomFilter synopsis (synID=2, uid=20, noOfP=1)
            //   params: [keyField, valueField, operationMode, expectedInsertions, falsePositiveRate]
            // ────────────────────────────────────────────────────────────────
            send(producer, requestTopic, "Forex",
                    """
                    {"dataSetkey":"Forex","requestID":1,"synopsisID":2,"uid":20,"streamID":"EURUSD","param":["StockID","price","Queryable","1000","0.01"],"noOfP":1}""");
            LOG.info("STEP 4: ADD BloomFilter uid=20 synID=2");
            producer.flush();
            Thread.sleep(3000);

            // ────────────────────────────────────────────────────────────────
            // STEP 5: Send more data (both CountMin uid=10 and BloomFilter uid=20 receive it)
            // ────────────────────────────────────────────────────────────────
            String[][] moreStocks = {
                    {"TSLA", "248.50"},
                    {"AAPL", "185.00"},
                    {"NVDA", "920.00"}
            };

            for (String[] stock : moreStocks) {
                String data = String.format(
                        "{\"dataSetkey\":\"Forex\",\"streamID\":\"EURUSD\",\"values\":{\"StockID\":\"%s\",\"price\":\"%s\"}}",
                        stock[0], stock[1]);
                send(producer, dataTopic, "Forex", data);
                LOG.info("STEP 5: DATA StockID={} price={}", stock[0], stock[1]);
                Thread.sleep(200);
            }
            producer.flush();
            Thread.sleep(3000);

            // ────────────────────────────────────────────────────────────────
            // STEP 6: ESTIMATE on BloomFilter — query "AAPL" membership
            //   Expected: true (AAPL was added)
            // ────────────────────────────────────────────────────────────────
            send(producer, requestTopic, "Forex",
                    """
                    {"dataSetkey":"Forex","requestID":3,"synopsisID":2,"uid":20,"streamID":"EURUSD","param":["AAPL"],"noOfP":1}""");
            LOG.info("STEP 6: ESTIMATE BloomFilter uid=20 query='AAPL' (expect true)");
            producer.flush();
            Thread.sleep(3000);

            // ────────────────────────────────────────────────────────────────
            // STEP 7: ADD a HyperLogLog synopsis (synID=4, uid=30, noOfP=1)
            //   params: [keyField, valueField, operationMode, relativeStdDev]
            // ────────────────────────────────────────────────────────────────
            send(producer, requestTopic, "Forex",
                    """
                    {"dataSetkey":"Forex","requestID":1,"synopsisID":4,"uid":30,"streamID":"EURUSD","param":["StockID","price","Queryable","0.05"],"noOfP":1}""");
            LOG.info("STEP 7: ADD HyperLogLog uid=30 synID=4");
            producer.flush();
            Thread.sleep(3000);

            // ────────────────────────────────────────────────────────────────
            // STEP 8: Send data with repeated items for HLL cardinality test
            //   Unique StockIDs: AAPL, GOOG, MSFT (3 unique out of 5 events)
            // ────────────────────────────────────────────────────────────────
            String[][] hllData = {
                    {"AAPL", "186.00"},
                    {"GOOG", "142.00"},
                    {"AAPL", "186.50"},
                    {"MSFT", "380.00"},
                    {"GOOG", "142.50"}
            };

            for (String[] stock : hllData) {
                String data = String.format(
                        "{\"dataSetkey\":\"Forex\",\"streamID\":\"EURUSD\",\"values\":{\"StockID\":\"%s\",\"price\":\"%s\"}}",
                        stock[0], stock[1]);
                send(producer, dataTopic, "Forex", data);
                LOG.info("STEP 8: DATA StockID={} price={}", stock[0], stock[1]);
                Thread.sleep(200);
            }
            producer.flush();
            Thread.sleep(3000);

            // ────────────────────────────────────────────────────────────────
            // STEP 9: ESTIMATE on HyperLogLog — query cardinality
            //   Expected: ~3 unique StockIDs (AAPL, GOOG, MSFT) added since HLL registration
            //   Note: HLL uses valueIndex (price), so it counts unique prices
            // ────────────────────────────────────────────────────────────────
            send(producer, requestTopic, "Forex",
                    """
                    {"dataSetkey":"Forex","requestID":3,"synopsisID":4,"uid":30,"streamID":"EURUSD","param":["cardinality"],"noOfP":1}""");
            LOG.info("STEP 9: ESTIMATE HyperLogLog uid=30 (expect ~5 unique prices)");
            producer.flush();
            Thread.sleep(3000);

            // ────────────────────────────────────────────────────────────────
            // STEP 10: DELETE the CountMin synopsis
            // ────────────────────────────────────────────────────────────────
            send(producer, requestTopic, "Forex",
                    """
                    {"dataSetkey":"Forex","requestID":2,"synopsisID":1,"uid":10,"streamID":"EURUSD","param":[],"noOfP":1}""");
            LOG.info("STEP 10: DELETE CountMin uid=10");
            producer.flush();
            Thread.sleep(3000);

            // ────────────────────────────────────────────────────────────────
            // STEP 11: ESTIMATE on deleted CountMin — should produce no output
            // ────────────────────────────────────────────────────────────────
            send(producer, requestTopic, "Forex",
                    """
                    {"dataSetkey":"Forex","requestID":3,"synopsisID":1,"uid":10,"streamID":"EURUSD","param":["AAPL"],"noOfP":1}""");
            LOG.info("STEP 11: ESTIMATE on deleted CountMin uid=10 (expect NO output)");
            producer.flush();
            Thread.sleep(3000);

            // ────────────────────────────────────────────────────────────────
            // STEP 12: ESTIMATE on BloomFilter — still alive after CountMin deletion
            //   query "NVDA" — was added in step 5
            //   Expected: true
            // ────────────────────────────────────────────────────────────────
            send(producer, requestTopic, "Forex",
                    """
                    {"dataSetkey":"Forex","requestID":3,"synopsisID":2,"uid":20,"streamID":"EURUSD","param":["NVDA"],"noOfP":1}""");
            LOG.info("STEP 12: ESTIMATE BloomFilter uid=20 query='NVDA' (expect true)");
            producer.flush();
            Thread.sleep(2000);

            LOG.info("=== All Layer 3 test messages sent. ===");
            LOG.info("Expected results:");
            LOG.info("  STEP 3:  CountMin AAPL count ≈ 549 (sum of prices: 182.50+183.00+184.00)");
            LOG.info("  STEP 6:  BloomFilter AAPL membership = true");
            LOG.info("  STEP 9:  HyperLogLog cardinality ≈ 5 unique prices");
            LOG.info("  STEP 11: No estimation (CountMin was deleted)");
            LOG.info("  STEP 12: BloomFilter NVDA membership = true");
        }
    }

    private static void send(KafkaProducer<String, String> producer,
                             String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value.strip()));
    }
}
