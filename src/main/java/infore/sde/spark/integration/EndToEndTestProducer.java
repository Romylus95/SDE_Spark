package infore.sde.spark.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * End-to-end test producer: exercises both GREEN and PURPLE paths.
 *
 * Test scenario:
 *   GREEN PATH (noOfP=1):
 *     1. ADD CountMin (synID=1, uid=10, noOfP=1)
 *     2. Send 5 data events
 *     3. ESTIMATE CountMin → expect result on estimation_topic
 *
 *   PURPLE PATH (noOfP=2):
 *     4. ADD CountMin (synID=1, uid=50, noOfP=2) → fans out to 2 keyed copies
 *     5. Send 5 data events (routed to both partitions)
 *     6. ESTIMATE on both partitions → partial results merged by ReduceAggregator
 *        → single merged result on estimation_topic
 *
 *   CLEANUP:
 *     7. DELETE uid=10 (green)
 *     8. DELETE uid=50 (purple — both partitions)
 *
 * Usage:
 *   java -cp sde-spark-1.0.0-SNAPSHOT.jar \
 *     infore.sde.spark.integration.EndToEndTestProducer localhost:9092
 */
public class EndToEndTestProducer {

    private static final Logger LOG = LoggerFactory.getLogger(EndToEndTestProducer.class);

    public static void main(String[] args) throws Exception {
        String brokers = args.length > 0 ? args[0] : "localhost:9092";
        String dataTopic = args.length > 1 ? args[1] : "data_topic";
        String requestTopic = args.length > 2 ? args[2] : "request_topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Batch processing time on local machine is ~10-30s per batch (with 4 shuffle partitions).
        // Delays must be long enough for each step to complete before the next begins.
        int batchWait = Integer.parseInt(System.getProperty("sde.test.batchWait", "40")) * 1000;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            LOG.info("=== E2E Test: GREEN + PURPLE paths (batchWait={}ms) ===", batchWait);

            // ════════════════════════════════════════════════════════════════
            //  GREEN PATH (noOfP=1)
            // ════════════════════════════════════════════════════════════════

            // Step 1: ADD CountMin (uid=10, noOfP=1) — GREEN path
            send(producer, requestTopic, "Forex",
                    "{\"dataSetkey\":\"Forex\",\"requestID\":1,\"synopsisID\":1,\"uid\":10,\"streamID\":\"EURUSD\",\"param\":[\"StockID\",\"price\",\"Queryable\",\"0.01\",\"0.99\",\"42\"],\"noOfP\":1}");
            LOG.info("STEP 1: ADD CountMin uid=10 noOfP=1 (GREEN)");
            producer.flush();
            Thread.sleep(batchWait);

            // Step 2: Send data
            sendData(producer, dataTopic, "AAPL", "182.50");
            sendData(producer, dataTopic, "AAPL", "183.00");
            sendData(producer, dataTopic, "GOOG", "141.20");
            sendData(producer, dataTopic, "AAPL", "184.00");
            sendData(producer, dataTopic, "MSFT", "378.90");
            LOG.info("STEP 2: Sent 5 data events (AAPL x3, GOOG x1, MSFT x1)");
            producer.flush();
            Thread.sleep(batchWait);

            // Step 3: ESTIMATE on CountMin uid=10 — query "AAPL"
            //   Expected: 549.0 (sum of AAPL prices: 182.50 + 183.00 + 184.00)
            send(producer, requestTopic, "Forex",
                    "{\"dataSetkey\":\"Forex\",\"requestID\":3,\"synopsisID\":1,\"uid\":10,\"streamID\":\"EURUSD\",\"param\":[\"AAPL\"],\"noOfP\":1}");
            LOG.info("STEP 3: ESTIMATE CountMin uid=10 query='AAPL' → expect ~549 (GREEN output)");
            producer.flush();
            Thread.sleep(batchWait);

            // ════════════════════════════════════════════════════════════════
            //  PURPLE PATH (noOfP=2)
            // ════════════════════════════════════════════════════════════════

            // Step 4: ADD CountMin (uid=50, noOfP=2) — PURPLE path
            //   RequestRouter fans out to: Forex_2_KEYED_0 and Forex_2_KEYED_1
            send(producer, requestTopic, "Forex",
                    "{\"dataSetkey\":\"Forex\",\"requestID\":1,\"synopsisID\":1,\"uid\":50,\"streamID\":\"EURUSD\",\"param\":[\"StockID\",\"price\",\"Queryable\",\"0.01\",\"0.99\",\"42\"],\"noOfP\":2}");
            LOG.info("STEP 4: ADD CountMin uid=50 noOfP=2 (PURPLE — fans out to 2 partitions)");
            producer.flush();
            Thread.sleep(batchWait);

            // Step 5: Send data with VARIED streamIDs so DataRouter hashes to BOTH partitions
            //   DataRouter: slot = abs(streamID.hashCode()) % noOfP
            //   Different streamIDs will hash to different slots
            sendDataWithStream(producer, dataTopic, "EURUSD", "AAPL", "185.00");
            sendDataWithStream(producer, dataTopic, "GBPUSD", "AAPL", "186.00");
            sendDataWithStream(producer, dataTopic, "USDJPY", "AAPL", "187.00");
            sendDataWithStream(producer, dataTopic, "EURUSD", "TSLA", "248.50");
            sendDataWithStream(producer, dataTopic, "GBPUSD", "NVDA", "920.00");
            sendDataWithStream(producer, dataTopic, "USDJPY", "GOOG", "142.00");
            LOG.info("STEP 5: Sent 6 data events with varied streamIDs for PURPLE partitions");
            producer.flush();
            Thread.sleep(batchWait);

            // Step 6: ESTIMATE on CountMin uid=50 — query "AAPL" on BOTH partitions
            //   Request fans out to noOfP=2 copies via RequestRouter
            //   Each partition estimates independently → ReduceAggregator merges (SimpleSumFunction)
            //   Expected: sum of AAPL counts across both partitions
            send(producer, requestTopic, "Forex",
                    "{\"dataSetkey\":\"Forex\",\"requestID\":3,\"synopsisID\":1,\"uid\":50,\"streamID\":\"EURUSD\",\"param\":[\"AAPL\"],\"noOfP\":2}");
            LOG.info("STEP 6: ESTIMATE CountMin uid=50 query='AAPL' noOfP=2 → expect PURPLE merged result");
            producer.flush();
            Thread.sleep(batchWait);

            // ════════════════════════════════════════════════════════════════
            //  CLEANUP
            // ════════════════════════════════════════════════════════════════

            // Step 7: DELETE uid=10 (green)
            send(producer, requestTopic, "Forex",
                    "{\"dataSetkey\":\"Forex\",\"requestID\":2,\"synopsisID\":1,\"uid\":10,\"streamID\":\"EURUSD\",\"param\":[],\"noOfP\":1}");
            LOG.info("STEP 7: DELETE uid=10 (GREEN)");

            // Step 8: DELETE uid=50 (purple — fans out to both partitions)
            send(producer, requestTopic, "Forex",
                    "{\"dataSetkey\":\"Forex\",\"requestID\":2,\"synopsisID\":1,\"uid\":50,\"streamID\":\"EURUSD\",\"param\":[],\"noOfP\":2}");
            LOG.info("STEP 8: DELETE uid=50 (PURPLE)");

            producer.flush();
            Thread.sleep(5000);

            LOG.info("=== All E2E test messages sent. ===");
            LOG.info("Expected results on estimation_topic:");
            LOG.info("  1. GREEN:  CountMin AAPL ≈ 549 (uid=10, noOfP=1)");
            LOG.info("  2. PURPLE: Merged CountMin AAPL sum across 2 partitions (uid=50, noOfP=2)");
            LOG.info("Check console output and kafka estimation_topic for results.");
        }
    }

    private static void sendData(KafkaProducer<String, String> producer,
                                  String topic, String stockId, String price) throws Exception {
        sendDataWithStream(producer, topic, "EURUSD", stockId, price);
    }

    private static void sendDataWithStream(KafkaProducer<String, String> producer,
                                            String topic, String streamID,
                                            String stockId, String price) throws Exception {
        String data = String.format(
                "{\"dataSetkey\":\"Forex\",\"streamID\":\"%s\",\"values\":{\"StockID\":\"%s\",\"price\":\"%s\"}}",
                streamID, stockId, price);
        send(producer, topic, "Forex", data);
        Thread.sleep(200);
    }

    private static void send(KafkaProducer<String, String> producer,
                             String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }
}
