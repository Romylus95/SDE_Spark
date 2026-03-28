package infore.sde.spark.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Locale;

/**
 * Configurable load test producer for SDE_Spark experimental evaluation.
 *
 * Generates synthetic stock market data at a controlled rate, registers
 * synopses, and periodically sends ESTIMATE requests.
 *
 * Replicates the experimental methodology from Kontaxakis (2020) Chapter 5:
 *   - Variable ingestion rate (1x, 2x, 5x, 10x)
 *   - Variable number of streams (50, 500, 5000)
 *   - Variable number of synopses
 *   - Periodic ESTIMATE requests for throughput measurement
 *
 * Usage:
 *   java -cp ... infore.sde.spark.integration.LoadTestProducer \
 *     --brokers localhost:9092 \
 *     --rate 1000 \
 *     --duration 300 \
 *     --num-streams 50 \
 *     --num-synopses 3 \
 *     --noop 1 \
 *     --estimate-interval 30
 *
 * Args:
 *   --brokers              Kafka bootstrap servers (default: localhost:9092)
 *   --data-topic           Data topic (default: data_topic)
 *   --request-topic        Request topic (default: request_topic)
 *   --rate                 Target messages per second (default: 1000)
 *   --duration             Test duration in seconds (default: 60)
 *   --num-streams          Number of distinct streamIDs (default: 50)
 *   --num-synopses         Number of synopses to register (default: 3)
 *   --noop                 Number of parallelism per synopsis (default: 1)
 *   --estimate-interval    Seconds between ESTIMATE requests (default: 30)
 *   --warmup               Warmup duration in seconds before measurement (default: 10)
 *   --synopsis-types       Comma-separated synopsisIDs to register (default: 1,2,4)
 */
public class LoadTestProducer {

    private static final Logger LOG = LoggerFactory.getLogger(LoadTestProducer.class);

    // Config
    private String brokers = "localhost:9092";
    private String dataTopic = "data_topic";
    private String requestTopic = "request_topic";
    private int rate = 1000;
    private int durationSec = 60;
    private int numStreams = 50;
    private int numSynopses = 3;
    private int noOfP = 1;
    private int estimateIntervalSec = 30;
    private int warmupSec = 10;
    private int[] synopsisTypes = {1, 2, 4}; // CountMin, BloomFilter, HyperLogLog

    // State
    private final List<String> streamIds = new ArrayList<>();
    private final List<String> stockSymbols = new ArrayList<>();
    private final Random random = new Random(42);
    private final AtomicLong sentCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public static void main(String[] args) throws Exception {
        LoadTestProducer producer = new LoadTestProducer();
        producer.parseArgs(args);
        producer.run();
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--brokers" -> brokers = args[++i];
                case "--data-topic" -> dataTopic = args[++i];
                case "--request-topic" -> requestTopic = args[++i];
                case "--rate" -> rate = Integer.parseInt(args[++i]);
                case "--duration" -> durationSec = Integer.parseInt(args[++i]);
                case "--num-streams" -> numStreams = Integer.parseInt(args[++i]);
                case "--num-synopses" -> numSynopses = Integer.parseInt(args[++i]);
                case "--noop" -> noOfP = Integer.parseInt(args[++i]);
                case "--estimate-interval" -> estimateIntervalSec = Integer.parseInt(args[++i]);
                case "--warmup" -> warmupSec = Integer.parseInt(args[++i]);
                case "--synopsis-types" -> {
                    String[] parts = args[++i].split(",");
                    synopsisTypes = new int[parts.length];
                    for (int j = 0; j < parts.length; j++) {
                        synopsisTypes[j] = Integer.parseInt(parts[j].trim());
                    }
                }
            }
        }
    }

    private void run() throws Exception {
        generateStreamIds();
        generateStockSymbols();

        LOG.info("════════════════════════════════════════════════════════════");
        LOG.info("  SDE_Spark Load Test Producer");
        LOG.info("  Brokers:           {}", brokers);
        LOG.info("  Data topic:        {}", dataTopic);
        LOG.info("  Request topic:     {}", requestTopic);
        LOG.info("  Target rate:       {} msg/sec", rate);
        LOG.info("  Duration:          {} sec", durationSec);
        LOG.info("  Warmup:            {} sec", warmupSec);
        LOG.info("  Streams:           {}", numStreams);
        LOG.info("  Synopses:          {}", numSynopses);
        LOG.info("  noOfP:             {}", noOfP);
        LOG.info("  Estimate interval: {} sec", estimateIntervalSec);
        LOG.info("  Synopsis types:    {}", Arrays.toString(synopsisTypes));
        LOG.info("════════════════════════════════════════════════════════════");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Phase 1: Register synopses
            LOG.info("Phase 1: Registering {} synopses...", numSynopses);
            registerSynopses(producer);
            producer.flush();
            Thread.sleep(5000); // Let the pipeline process ADDs

            // Phase 2: Warmup
            LOG.info("Phase 2: Warmup ({} sec at {} msg/sec)...", warmupSec, rate);
            sendDataBurst(producer, warmupSec);
            LOG.info("Warmup complete. {} messages sent.", sentCount.get());
            sentCount.set(0); // Reset counter for measurement phase

            // Phase 3: Measured data sending with periodic estimates
            LOG.info("Phase 3: Measurement ({} sec at {} msg/sec)...", durationSec, rate);
            long measureStart = System.currentTimeMillis();
            sendDataWithEstimates(producer, durationSec);
            long measureEnd = System.currentTimeMillis();

            producer.flush();

            // Phase 4: Cleanup
            LOG.info("Phase 4: Cleanup — deleting synopses...");
            deleteSynopses(producer);
            producer.flush();

            // Report
            long sent = sentCount.get();
            long elapsed = measureEnd - measureStart;
            double actualRate = elapsed > 0 ? (sent * 1000.0 / elapsed) : 0;

            LOG.info("════════════════════════════════════════════════════════════");
            LOG.info("  LOAD TEST COMPLETE");
            LOG.info("  Messages sent:     {}", sent);
            LOG.info("  Errors:            {}", errorCount.get());
            LOG.info("  Elapsed:           {} ms ({} sec)", elapsed, elapsed / 1000);
            LOG.info("  Actual send rate:  {} msg/sec", String.format("%.1f", actualRate));
            LOG.info("  Target rate:       {} msg/sec", rate);
            LOG.info("════════════════════════════════════════════════════════════");
        }
    }

    private void registerSynopses(KafkaProducer<String, String> producer) {
        int uid = 100;
        for (int i = 0; i < numSynopses; i++) {
            int typeIdx = i % synopsisTypes.length;
            int synopsisID = synopsisTypes[typeIdx];
            String[] params = getSynopsisParams(synopsisID);

            String request = String.format(
                    "{\"dataSetkey\":\"Forex\",\"requestID\":1,\"synopsisID\":%d,\"uid\":%d," +
                    "\"streamID\":\"ALL\",\"param\":[%s],\"noOfP\":%d}",
                    synopsisID, uid, formatParams(params), noOfP);

            producer.send(new ProducerRecord<>(requestTopic, "Forex", request));
            LOG.info("Registered synopsis uid={} type={} noOfP={}", uid, synopsisID, noOfP);
            uid++;
        }
    }

    private void deleteSynopses(KafkaProducer<String, String> producer) {
        int uid = 100;
        for (int i = 0; i < numSynopses; i++) {
            int typeIdx = i % synopsisTypes.length;
            int synopsisID = synopsisTypes[typeIdx];

            String request = String.format(
                    "{\"dataSetkey\":\"Forex\",\"requestID\":2,\"synopsisID\":%d,\"uid\":%d," +
                    "\"streamID\":\"ALL\",\"param\":[],\"noOfP\":%d}",
                    synopsisID, uid, noOfP);

            producer.send(new ProducerRecord<>(requestTopic, "Forex", request));
            uid++;
        }
        LOG.info("Deleted {} synopses", numSynopses);
    }

    private void sendDataBurst(KafkaProducer<String, String> producer, int seconds) throws Exception {
        long endTime = System.currentTimeMillis() + (seconds * 1000L);
        long intervalNanos = 1_000_000_000L / rate;

        while (System.currentTimeMillis() < endTime) {
            long batchStart = System.nanoTime();
            // Send a batch of messages, then sleep for the remaining time
            int batchSize = Math.min(rate / 10, 1000); // Send in chunks of ~100ms worth
            if (batchSize < 1) batchSize = 1;

            for (int i = 0; i < batchSize; i++) {
                sendOneDatapoint(producer);
            }
            producer.flush();

            long batchElapsed = System.nanoTime() - batchStart;
            long targetNanos = batchSize * intervalNanos;
            long sleepNanos = targetNanos - batchElapsed;
            if (sleepNanos > 0) {
                Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
            }
        }
    }

    private void sendDataWithEstimates(KafkaProducer<String, String> producer, int seconds) throws Exception {
        long endTime = System.currentTimeMillis() + (seconds * 1000L);
        long lastEstimate = System.currentTimeMillis();
        long intervalNanos = 1_000_000_000L / rate;
        long reportInterval = 10_000; // Log progress every 10 seconds
        long lastReport = System.currentTimeMillis();

        while (System.currentTimeMillis() < endTime) {
            long batchStart = System.nanoTime();
            int batchSize = Math.min(rate / 10, 1000);
            if (batchSize < 1) batchSize = 1;

            for (int i = 0; i < batchSize; i++) {
                sendOneDatapoint(producer);
            }
            producer.flush();

            // Periodic ESTIMATE
            long now = System.currentTimeMillis();
            if (now - lastEstimate >= estimateIntervalSec * 1000L) {
                sendEstimateRequests(producer);
                lastEstimate = now;
            }

            // Progress report
            if (now - lastReport >= reportInterval) {
                long sent = sentCount.get();
                long elapsed = now - (endTime - seconds * 1000L);
                double actualRate = elapsed > 0 ? (sent * 1000.0 / elapsed) : 0;
                int remaining = (int) ((endTime - now) / 1000);
                LOG.info("Progress: {} sent, {} msg/sec actual, {}s remaining",
                        sent, String.format("%.0f", actualRate), remaining);
                lastReport = now;
            }

            long batchElapsed = System.nanoTime() - batchStart;
            long targetNanos = batchSize * intervalNanos;
            long sleepNanos = targetNanos - batchElapsed;
            if (sleepNanos > 0) {
                Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
            }
        }
    }

    private void sendOneDatapoint(KafkaProducer<String, String> producer) {
        String streamId = streamIds.get(random.nextInt(streamIds.size()));
        String stockId = stockSymbols.get(random.nextInt(stockSymbols.size()));
        double price = 50.0 + random.nextDouble() * 950.0; // Random price 50-1000

        String data = String.format(Locale.US,
                "{\"dataSetkey\":\"Forex\",\"streamID\":\"%s\",\"values\":{\"StockID\":\"%s\",\"price\":\"%.2f\"}}",
                streamId, stockId, price);

        producer.send(new ProducerRecord<>(dataTopic, "Forex", data), (metadata, exception) -> {
            if (exception != null) {
                errorCount.incrementAndGet();
            }
        });
        sentCount.incrementAndGet();
    }

    private void sendEstimateRequests(KafkaProducer<String, String> producer) {
        // Send one ESTIMATE per registered synopsis for a random stock
        int uid = 100;
        String queryStock = stockSymbols.get(random.nextInt(stockSymbols.size()));

        for (int i = 0; i < numSynopses; i++) {
            int typeIdx = i % synopsisTypes.length;
            int synopsisID = synopsisTypes[typeIdx];

            String request = String.format(
                    "{\"dataSetkey\":\"Forex\",\"requestID\":3,\"synopsisID\":%d,\"uid\":%d," +
                    "\"streamID\":\"ALL\",\"param\":[\"%s\"],\"noOfP\":%d}",
                    synopsisID, uid, queryStock, noOfP);

            producer.send(new ProducerRecord<>(requestTopic, "Forex", request));
            uid++;
        }
        LOG.info("Sent {} ESTIMATE requests for '{}'", numSynopses, queryStock);
    }

    private void generateStreamIds() {
        // Generate stream IDs like currency pairs or market identifiers
        String[] bases = {"EUR", "GBP", "USD", "JPY", "CHF", "AUD", "CAD", "NZD", "SEK", "NOK",
                          "DKK", "HKD", "SGD", "KRW", "TWD", "BRL", "MXN", "ZAR", "TRY", "INR",
                          "CNY", "RUB", "PLN", "CZK", "HUF"};

        for (int i = 0; i < numStreams; i++) {
            if (i < bases.length * (bases.length - 1)) {
                int a = i / (bases.length - 1);
                int b = i % (bases.length - 1);
                if (b >= a) b++;
                streamIds.add(bases[a] + bases[b]);
            } else {
                streamIds.add("STREAM_" + i);
            }
        }
        LOG.info("Generated {} stream IDs", streamIds.size());
    }

    private void generateStockSymbols() {
        // Real-ish stock symbols
        String[] symbols = {
            "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "BRK", "UNH", "JNJ",
            "V", "XOM", "WMT", "JPM", "PG", "MA", "HD", "CVX", "MRK", "ABBV",
            "LLY", "PEP", "KO", "COST", "AVGO", "TMO", "MCD", "WFC", "CRM", "ACN",
            "ABT", "DHR", "LIN", "ADBE", "TXN", "CMCSA", "NEE", "PM", "VZ", "RTX",
            "BMY", "INTC", "AMD", "QCOM", "HON", "UNP", "LOW", "SBUX", "GS", "SPGI"
        };
        stockSymbols.addAll(Arrays.asList(symbols));
    }

    private String[] getSynopsisParams(int synopsisID) {
        return switch (synopsisID) {
            case 1 -> new String[]{"StockID", "price", "Queryable", "0.002", "0.01", "42"}; // CountMin
            case 2 -> new String[]{"StockID", "price", "0", "10000", "3"};                   // BloomFilter
            case 3 -> new String[]{"StockID", "price", "0", "100", "5"};                      // AMS
            case 4 -> new String[]{"StockID", "price", "0", "0.05"};                          // HyperLogLog
            default -> new String[]{"StockID", "price", "0"};
        };
    }

    private String formatParams(String[] params) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < params.length; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(params[i]).append("\"");
        }
        return sb.toString();
    }
}
