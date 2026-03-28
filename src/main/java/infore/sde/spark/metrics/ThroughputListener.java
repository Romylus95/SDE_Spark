package infore.sde.spark.metrics;

import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Captures per-batch streaming metrics and writes them to a CSV file.
 *
 * Metrics captured per batch:
 *   - timestamp, batchId, numInputRows, inputRowsPerSecond,
 *     processedRowsPerSecond, batchDurationMs
 *
 * Usage:
 *   ThroughputListener listener = new ThroughputListener("results/experiment-A.csv");
 *   spark.streams().addListener(listener);
 *   // ... run pipeline ...
 *   listener.printSummary();
 *
 * Visible in Spark UI and persisted to CSV for graphing.
 */
public class ThroughputListener extends StreamingQueryListener {

    private static final Logger LOG = LoggerFactory.getLogger(ThroughputListener.class);
    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private final String csvPath;
    private PrintWriter writer;

    // Running stats
    private final AtomicLong totalRows = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong totalDurationMs = new AtomicLong(0);
    private volatile double peakProcessedPerSec = 0;
    private volatile long startTimeMs = 0;

    public ThroughputListener(String csvPath) {
        this.csvPath = csvPath;
    }

    @Override
    public void onQueryStarted(QueryStartedEvent event) {
        startTimeMs = System.currentTimeMillis();
        try {
            Path path = Paths.get(csvPath);
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            writer = new PrintWriter(new FileWriter(csvPath, false));
            writer.println("timestamp,batch_id,num_input_rows,input_rows_per_sec,processed_rows_per_sec,batch_duration_ms");
            writer.flush();
            LOG.info("ThroughputListener: writing metrics to {}", csvPath);
        } catch (IOException e) {
            LOG.error("ThroughputListener: failed to open CSV file {}: {}", csvPath, e.getMessage());
        }
        LOG.info("ThroughputListener: query '{}' started (id={})",
                event.name(), event.id());
    }

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        StreamingQueryProgress progress = event.progress();

        long numInputRows = progress.numInputRows();
        double inputRowsPerSec = progress.inputRowsPerSecond();
        double processedRowsPerSec = progress.processedRowsPerSecond();
        long batchId = progress.batchId();

        // Extract batch duration
        Map<String, Long> durations = progress.durationMs();
        long batchDurationMs = durations.getOrDefault("triggerExecution", 0L);

        totalRows.addAndGet(numInputRows);
        totalBatches.incrementAndGet();
        totalDurationMs.addAndGet(batchDurationMs);
        if (processedRowsPerSec > peakProcessedPerSec) {
            peakProcessedPerSec = processedRowsPerSec;
        }

        String timestamp = TS_FMT.format(Instant.now());

        if (writer != null) {
            writer.printf("%s,%d,%d,%.2f,%.2f,%d%n",
                    timestamp, batchId, numInputRows,
                    inputRowsPerSec, processedRowsPerSec, batchDurationMs);
            writer.flush();
        }

        if (numInputRows > 0) {
            LOG.info("Batch {}: {} rows, input={}/s, processed={}/s, duration={}ms",
                    batchId, numInputRows,
                    String.format("%.1f", inputRowsPerSec),
                    String.format("%.1f", processedRowsPerSec),
                    batchDurationMs);
        }
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
        LOG.info("ThroughputListener: query terminated (id={})", event.id());
        printSummary();
        if (writer != null) {
            writer.close();
        }
    }

    /**
     * Prints aggregate statistics. Call after the experiment completes.
     */
    public void printSummary() {
        long elapsed = System.currentTimeMillis() - startTimeMs;
        long rows = totalRows.get();
        long batches = totalBatches.get();
        double avgThroughput = elapsed > 0 ? (rows * 1000.0 / elapsed) : 0;
        double avgBatchDuration = batches > 0 ? (totalDurationMs.get() / (double) batches) : 0;

        LOG.info("════════════════════════════════════════════════════════════");
        LOG.info("  EXPERIMENT SUMMARY");
        LOG.info("  Total rows processed:    {}", rows);
        LOG.info("  Total batches:           {}", batches);
        LOG.info("  Elapsed time:            {} ms ({} sec)", elapsed, elapsed / 1000);
        LOG.info("  Avg throughput:          {} rows/sec", String.format("%.1f", avgThroughput));
        LOG.info("  Peak throughput:         {} rows/sec", String.format("%.1f", peakProcessedPerSec));
        LOG.info("  Avg batch duration:      {} ms", String.format("%.1f", avgBatchDuration));
        LOG.info("  Results written to:      {}", csvPath);
        LOG.info("════════════════════════════════════════════════════════════");

        // Also write summary to a separate file
        try (PrintWriter summary = new PrintWriter(new FileWriter(csvPath + ".summary.txt"))) {
            summary.printf("total_rows=%d%n", rows);
            summary.printf("total_batches=%d%n", batches);
            summary.printf("elapsed_ms=%d%n", elapsed);
            summary.printf("avg_throughput_per_sec=%.2f%n", avgThroughput);
            summary.printf("peak_throughput_per_sec=%.2f%n", peakProcessedPerSec);
            summary.printf("avg_batch_duration_ms=%.2f%n", avgBatchDuration);
        } catch (IOException e) {
            LOG.warn("Failed to write summary file: {}", e.getMessage());
        }
    }

    public long getTotalRows() { return totalRows.get(); }
    public long getTotalBatches() { return totalBatches.get(); }
    public double getPeakProcessedPerSec() { return peakProcessedPerSec; }
}
