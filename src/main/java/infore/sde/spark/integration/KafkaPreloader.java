package infore.sde.spark.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaPreloader {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPreloader.class);

    private static final String[] STOCK_SYMBOLS = {
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "BRK",  "JPM",  "JNJ",
        "V",    "PG",   "UNH",  "HD",   "MA",   "DIS",  "PYPL", "ADBE", "NFLX", "CMCSA",
        "VZ",   "T",    "INTC", "CSCO", "PFE",  "MRK",  "ABT",  "TMO",  "ACN",  "AVGO",
        "TXN",  "QCOM", "COST", "NKE",  "WMT",  "MCD",  "BMY",  "AMGN", "LLY",  "ORCL",
        "IBM",  "GE",   "MMM",  "CAT",  "BA",   "RTX",  "HON",  "UPS",  "FDX",  "SBUX"
    };

    private String brokers      = "localhost:9092";
    private String dataTopic    = "data_topic";
    private String requestTopic = "request_topic";
    private String datasetKey   = "Forex";
    private int    numStreams    = 50;
    private int    numSynopses  = 1;
    private int    noop         = 1;
    private int    baseMessages = 50000;
    private int    multiplier   = 100;
    private int[]  synopsisTypes = {1};

    public static void main(String[] args) throws Exception {
        KafkaPreloader preloader = new KafkaPreloader();
        preloader.parseArgs(args);
        preloader.run();
    }

    private void run() throws Exception {
        LOG.info("=== KafkaPreloader starting ===");
        LOG.info("  Brokers:        {}", brokers);
        LOG.info("  Data topic:     {}", dataTopic);
        LOG.info("  Request topic:  {}", requestTopic);
        LOG.info("  Dataset key:    {}", datasetKey);
        LOG.info("  Num streams:    {}", numStreams);
        LOG.info("  Synopsis types: {}", Arrays.toString(synopsisTypes));
        LOG.info("  noOfP:          {}", noop);
        LOG.info("  Base messages:  {}", baseMessages);
        LOG.info("  Multiplier:     {}", multiplier);
        LOG.info("  Total messages: {}", (long) baseMessages * multiplier);

        phase1RegisterSynopses();
        List<String> base = phase2GenerateBaseDataset();
        phase3WriteToKafka(base);
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--brokers":
                    brokers = args[++i];
                    break;
                case "--data-topic":
                    dataTopic = args[++i];
                    break;
                case "--request-topic":
                    requestTopic = args[++i];
                    break;
                case "--dataset-key":
                    datasetKey = args[++i];
                    break;
                case "--num-streams":
                    numStreams = Integer.parseInt(args[++i]);
                    break;
                case "--num-synopses":
                    numSynopses = Integer.parseInt(args[++i]);
                    break;
                case "--synopsis-types":
                    String[] parts = args[++i].split(",");
                    synopsisTypes = new int[parts.length];
                    for (int j = 0; j < parts.length; j++) {
                        synopsisTypes[j] = Integer.parseInt(parts[j].trim());
                    }
                    break;
                case "--noop":
                    noop = Integer.parseInt(args[++i]);
                    break;
                case "--base-messages":
                    baseMessages = Integer.parseInt(args[++i]);
                    break;
                case "--multiplier":
                    multiplier = Integer.parseInt(args[++i]);
                    break;
                default:
                    break;
            }
        }
    }

    private void phase1RegisterSynopses() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks",             "all");
        props.put("linger.ms",        "0");
        props.put("retries",          "3");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int registered = 0;

        try {
            for (int type : synopsisTypes) {
                String[] params = getSynopsisParams(type);
                for (int i = 1; i <= numSynopses; i++) {
                    String json = buildAddRequestJson(i, type, params);
                    producer.send(new ProducerRecord<>(requestTopic, datasetKey, json)).get();
                    LOG.info("Registered synopsis uid={} type={} noOfP={}", i, type, noop);
                    registered++;
                }
            }
        } finally {
            producer.close();
        }

        LOG.info("Phase 1 complete: {} ADD request(s) sent. Sleeping 3s...", registered);
        Thread.sleep(3000);
    }

    private List<String> phase2GenerateBaseDataset() {
        LOG.info("Phase 2: generating {} base messages...", baseMessages);
        List<String> base = new ArrayList<>(baseMessages);
        Random rng = new Random(42);

        for (int i = 0; i < baseMessages; i++) {
            String streamId = "stream_" + (i % numStreams);
            String stockId  = STOCK_SYMBOLS[rng.nextInt(STOCK_SYMBOLS.length)];
            double price    = 1.0 + rng.nextDouble() * 499.0;
            base.add(buildDatapointJson(streamId, stockId, price));
        }

        LOG.info("Phase 2 complete: {} messages pre-serialized (~{} MB).",
                baseMessages,
                String.format(Locale.US, "%.1f", baseMessages * 100.0 / 1_000_000.0));
        return base;
    }

    private void phase3WriteToKafka(List<String> base) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer",    "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",  "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks",              "1");
        props.put("batch.size",        "65536");
        props.put("linger.ms",         "5");
        props.put("buffer.memory",     "134217728");
        props.put("compression.type",  "lz4");
        props.put("retries",           "3");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        AtomicLong sentCount  = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);
        long startMs = System.currentTimeMillis();

        LOG.info("Phase 3: writing {} passes of {} messages ({} total)...",
                multiplier, base.size(), (long) multiplier * base.size());

        try {
            for (int pass = 1; pass <= multiplier; pass++) {
                for (String json : base) {
                    producer.send(
                        new ProducerRecord<>(dataTopic, null, json),
                        (metadata, exception) -> {
                            if (exception != null) {
                                errorCount.incrementAndGet();
                            } else {
                                sentCount.incrementAndGet();
                            }
                        }
                    );
                }

                if (multiplier <= 10 || pass % 10 == 0) {
                    producer.flush();
                    long elapsed = System.currentTimeMillis() - startMs;
                    long sent    = sentCount.get();
                    double rate  = elapsed > 0 ? sent * 1000.0 / elapsed : 0;
                    LOG.info("Pass {}/{} — {} messages sent ({} msg/sec)",
                            pass, multiplier, sent,
                            String.format(Locale.US, "%.0f", rate));
                }
            }

            producer.flush();
        } finally {
            producer.close();
        }

        long totalMs = System.currentTimeMillis() - startMs;
        long total   = (long) baseMessages * multiplier;
        double rate  = totalMs > 0 ? total * 1000.0 / totalMs : 0;

        LOG.info("=== KafkaPreloader complete ===");
        LOG.info("  Synopses registered:  {}", synopsisTypes.length * numSynopses);
        LOG.info("  Base dataset size:    {}", baseMessages);
        LOG.info("  Multiplier:           {}", multiplier);
        LOG.info("  Total messages:       {}", total);
        LOG.info("  Time elapsed:         {} sec",
                String.format(Locale.US, "%.1f", totalMs / 1000.0));
        LOG.info("  Preload throughput:   {} msg/sec",
                String.format(Locale.US, "%.0f", rate));
        LOG.info("  Errors:               {}", errorCount.get());
        LOG.info("");
        LOG.info("Kafka is ready. Start the Spark pipeline with:");
        LOG.info("  --starting-offsets earliest");
    }

    private String buildDatapointJson(String streamId, String stockId, double price) {
        return String.format(Locale.US,
                "{\"dataSetkey\":\"%s\",\"streamID\":\"%s\",\"values\":{\"StockID\":\"%s\",\"price\":\"%.2f\"}}",
                datasetKey, streamId, stockId, price);
    }

    private String buildAddRequestJson(int uid, int synopsisId, String[] params) {
        StringBuilder paramArray = new StringBuilder("[");
        for (int i = 0; i < params.length; i++) {
            if (i > 0) paramArray.append(",");
            paramArray.append("\"").append(params[i]).append("\"");
        }
        paramArray.append("]");

        return String.format(Locale.US,
                "{\"dataSetkey\":\"%s\",\"requestID\":1,\"synopsisID\":%d,\"uid\":%d," +
                "\"streamID\":\"ALL\",\"param\":%s,\"noOfP\":%d}",
                datasetKey, synopsisId, uid, paramArray.toString(), noop);
    }

    private String[] getSynopsisParams(int synopsisId) {
        switch (synopsisId) {
            case 1:
                return new String[]{"StockID", "price", "Queryable", "0.002", "0.01", "42"};
            case 2:
                return new String[]{"StockID", "price", "0", "10000", "0.01"};
            case 3:
                return new String[]{"StockID", "price", "0", "100", "5"};
            case 4:
                return new String[]{"StockID", "price", "0", "0.05"};
            default:
                throw new IllegalArgumentException("Unknown synopsis type: " + synopsisId);
        }
    }
}
