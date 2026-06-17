package infore.sde.spark;

import infore.sde.spark.aggregation.ReduceAggregator;
import infore.sde.spark.config.SDEConfig;
import infore.sde.spark.ingestion.KafkaIngestionLayer;
import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import infore.sde.spark.metrics.PipelineMetrics;
import infore.sde.spark.metrics.ThroughputListener;
import infore.sde.spark.output.KafkaOutputLayer;
import infore.sde.spark.processing.InputEvent;
import infore.sde.spark.processing.SynopsisProcessor;
import infore.sde.spark.processing.SynopsisProcessorState;
import infore.sde.spark.routing.StatelessRouter;

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
 * SDE_Spark — Synopsis Data Engine, Spark Edition (Hybrid Architecture).
 *
 * Main entry point. Hybrid pipeline: stateless router + single stateful synopsis
 * processor + stateless aggregator. Compared to the baseline:
 *   - DataRouter (stateful, N state partitions) → StatelessRouter (no state)
 *   - SynopsisProcessor (stateful, N state partitions) → kept, keyed by KEYED slot key
 *   - ReduceAggregator (stateful, N state partitions) → kept stateless (from Priority 2)
 *
 * Result: 1 stateful operator instead of 3 (vs baseline), full horizontal scalability
 * via --num-slots N (each slot = independent Spark partition + executor assignment).
 *
 * Pipeline:
 *   Layer 1: KafkaIngestionLayer    — read raw JSON from Kafka
 *   Layer 2: StatelessRouter        — route/fan-out events by KEYED slot key (no state)
 *   Layer 3: SynopsisProcessor      — synopsis lifecycle per slot (1 stateful operator)
 *   Layer 4: ReduceAggregator       — merge N partial estimates (no state)
 *   Layer 5: KafkaOutputLayer       — write estimations to Kafka
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

        String metricsPath = System.getProperty("sde.metrics.path", "results/throughput.csv");
        ThroughputListener throughputListener = new ThroughputListener(metricsPath, config.getMaxOffsetsPerTrigger(), config.getIngestionMultiplier());
        spark.streams().addListener(throughputListener);

        LOG.info("SDE_Spark (hybrid) starting: dataTopic={}, requestTopic={}, outputTopic={}, brokers={}, numSlots={}",
                config.getDataTopic(), config.getRequestTopic(),
                config.getOutputTopic(), config.getKafkaBrokers(), config.getNumSlots());

        // ──── Layer 1: Ingestion ────
        KafkaIngestionLayer ingestion = new KafkaIngestionLayer(spark, config);
        Dataset<Datapoint> dataStream = ingestion.readDataStream();
        Dataset<Request> requestStream = ingestion.readRequestStream();

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

        // ──── Layer 2: Stateless routing — no HDFS state writes ────
        // Union data + requests into a single InputEvent stream, then apply the
        // stateless router. For numSlots=1: pass through (original key unchanged).
        // For numSlots=N: data routed to one KEYED slot; requests fanned out to N slots.
        Dataset<InputEvent> taggedData = dataStream.map(
                (MapFunction<Datapoint, InputEvent>) InputEvent::data,
                Encoders.kryo(InputEvent.class));
        Dataset<InputEvent> taggedRequests = requestStream.map(
                (MapFunction<Request, InputEvent>) InputEvent::request,
                Encoders.kryo(InputEvent.class));

        Dataset<InputEvent> routed = taggedData.union(taggedRequests)
                .flatMap(new StatelessRouter(config.getNumSlots()),
                        Encoders.kryo(InputEvent.class));

        // ──── Layer 3: Synopsis lifecycle — one stateful operator ────
        // Keyed by the routing key (KEYED slot key for numSlots>1, base key for numSlots=1).
        // spark.sql.shuffle.partitions should be set to numSlots via spark-submit so that
        // Spark assigns one executor per slot, enabling horizontal scaling.
        KeyValueGroupedDataset<String, InputEvent> groupedBySlotKey = routed.groupByKey(
                (MapFunction<InputEvent, String>) InputEvent::getRoutingKey,
                Encoders.STRING());

        Dataset<Estimation> partialEstimations = groupedBySlotKey.flatMapGroupsWithState(
                new SynopsisProcessor(config, metrics),
                OutputMode.Append(),
                Encoders.kryo(SynopsisProcessorState.class),
                Encoders.kryo(Estimation.class),
                GroupStateTimeout.ProcessingTimeTimeout());

        // ──── Layer 4: Stateless aggregation (PURPLE path) ────
        // For numSlots=1: each estimation group has exactly 1 element (noOfP=1),
        // ReduceAggregator passes it through immediately — no overhead.
        // For numSlots=N: N partial estimations per uid are merged in-batch.
        Dataset<Estimation> allEstimations = partialEstimations
                .groupByKey((MapFunction<Estimation, Integer>) Estimation::getUid, Encoders.INT())
                .flatMapGroups(new ReduceAggregator(), Encoders.kryo(Estimation.class));

        // ──── Layer 5: Output ────
        KafkaOutputLayer outputLayer = new KafkaOutputLayer(config);
        StreamingQuery query = outputLayer.write(allEstimations, "sde-output");

        LOG.info("SDE_Spark (hybrid) pipeline started. Awaiting termination...");

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
