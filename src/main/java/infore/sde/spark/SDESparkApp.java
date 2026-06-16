package infore.sde.spark;

import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.ingestion.KafkaIngestionLayer;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import infore.sde.spark.metrics.PipelineMetrics;
import infore.sde.spark.metrics.ThroughputListener;
import infore.sde.spark.output.KafkaOutputLayer;
import infore.sde.spark.processing.CombinedProcessor;
import infore.sde.spark.processing.CombinedState;
import infore.sde.spark.processing.InputEvent;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SDE_Spark — Synopsis Data Engine, Spark Edition.
 *
 * Main entry point. Wires together the optimized pipeline:
 *   Layer 1:   KafkaIngestionLayer  — read raw JSON from Kafka
 *   Layers 2+3: CombinedProcessor  — routing + synopsis lifecycle + inline PURPLE reduction
 *                                    (single flatMapGroupsWithState keyed by base dataSetKey)
 *   Layer 6:   KafkaOutputLayer    — write estimations to Kafka
 */
public class SDESparkApp {

    private static final Logger LOG = LoggerFactory.getLogger(SDESparkApp.class);

    public static void main(String[] args) throws Exception {
        SDEConfig config = SDEConfig.fromArgs(args);

        SparkSession spark = SparkSession.builder()
                .appName("SDE_Spark")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        PipelineMetrics metrics = new PipelineMetrics(spark);

        // Metrics listener — writes per-batch throughput to CSV
        String metricsPath = System.getProperty("sde.metrics.path", "results/throughput.csv");
        ThroughputListener throughputListener = new ThroughputListener(metricsPath, config.getMaxOffsetsPerTrigger(), config.getIngestionMultiplier());
        spark.streams().addListener(throughputListener);

        LOG.info("SDE_Spark starting with config: dataTopic={}, requestTopic={}, outputTopic={}, brokers={}",
                config.getDataTopic(), config.getRequestTopic(),
                config.getOutputTopic(), config.getKafkaBrokers());

        // ──── Layer 1: Ingestion ────
        KafkaIngestionLayer ingestion = new KafkaIngestionLayer(spark, config);
        Dataset<Datapoint> dataStream = ingestion.readDataStream();
        Dataset<Request> requestStream = ingestion.readRequestStream();

        // In-memory ingestion multiplier (Kontaxakis approach): each Datapoint read from Kafka
        // is duplicated N times in memory before reaching the routing layer. This makes workers
        // compute-bound without changing Kafka throughput or state size, enabling genuine
        // worker scaling experiments. Applied to data only — requests must never be multiplied.
        if (config.getIngestionMultiplier() > 1) {
            final int mult = config.getIngestionMultiplier();
            dataStream = dataStream.flatMap(
                    (FlatMapFunction<Datapoint, Datapoint>) dp -> {
                        List<Datapoint> copies = new ArrayList<>(mult);
                        for (int i = 0; i < mult; i++) copies.add(dp);
                        return copies.iterator();
                    },
                    Encoders.kryo(Datapoint.class));
        }

        // ──── Layers 2+3: Combined routing + synopsis lifecycle ────
        // Union data and requests into a single tagged stream, then process
        // both in one stateful operator keyed by base dataSetKey.
        // GREEN and PURPLE estimation results are emitted directly — no downstream
        // shuffle, fan-out, or ReduceAggregator step needed.
        Dataset<InputEvent> taggedData = dataStream.map(
                (MapFunction<Datapoint, InputEvent>) InputEvent::data,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> taggedRequests = requestStream.map(
                (MapFunction<Request, InputEvent>) InputEvent::request,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> combined = taggedData.union(taggedRequests);

        KeyValueGroupedDataset<String, InputEvent> groupedByBaseKey = combined.groupByKey(
                (MapFunction<InputEvent, String>) InputEvent::getDataSetKey,
                Encoders.STRING());

        Dataset<Estimation> allEstimations = groupedByBaseKey.flatMapGroupsWithState(
                new CombinedProcessor(config, metrics),
                OutputMode.Append(),
                Encoders.kryo(CombinedState.class),
                Encoders.kryo(Estimation.class),
                GroupStateTimeout.ProcessingTimeTimeout());

        // ──── Layer 6: Output ────
        KafkaOutputLayer outputLayer = new KafkaOutputLayer(config);
        StreamingQuery query = outputLayer.write(allEstimations, "sde-output");

        LOG.info("SDE_Spark pipeline started. Awaiting termination...");

        // Graceful shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received. Stopping pipeline gracefully...");
            try {
                query.stop();
                spark.stop();
                LOG.info("Pipeline stopped gracefully.");
            } catch (Exception e) {
                LOG.error("Error during graceful shutdown", e);
            }
        }));

        try {
            query.awaitTermination();
        } catch (Exception e) {
            LOG.error("Pipeline terminated with error", e);
        } finally {
            spark.stop();
        }
    }
}
