package infore.sde.spark.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streaminer.stream.frequency.AMSSketch;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;

/**
 * AMS Sketch synopsis — frequency moment estimation.
 * Estimates the second frequency moment (F2) of a data stream, which measures
 * the "skewness" of the distribution. Uses the streaminer AMSSketch implementation.
 *
 * The AMSSketch object is transient (not directly serializable by Kryo).
 * State is preserved via reflection-based snapshot/restore: internal fields
 * (count, counts, test) are extracted before each checkpoint and restored on access.
 * Reflection is required because AMSSketch does not expose its internal state.
 */
public class AMSsynopsis extends Synopsis {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AMSsynopsis.class);

    private transient AMSSketch ams;
    private int buckets;
    private int depth;

    // Snapshot fields — Kryo serializes these directly.
    // Kept in sync via snapshotState() / restoreState().
    private int snapshotCount;
    private int[] snapshotCounts;
    private long[][] snapshotTest;

    /**
     * @param uid        unique synopsis instance ID
     * @param parameters [0]=keyField, [1]=valueField, [2]=operationMode,
     *                   [3]=buckets (int), [4]=depth (int)
     */
    public AMSsynopsis(int uid, String[] parameters) {
        super(uid, parameters[0], parameters[1], parameters[2]);
        this.buckets = Integer.parseInt(parameters[3]);
        this.depth = Integer.parseInt(parameters[4]);
        this.ams = new AMSSketch(buckets, depth);
    }

    private void ensureAms() {
        if (ams == null) {
            ams = new AMSSketch(buckets, depth);
            restoreFromSnapshot();
        }
    }

    @Override
    public void snapshotState() {
        if (ams == null) return;
        try {
            snapshotCount = getField(ams, "count");
            snapshotCounts = getField(ams, "counts");
            snapshotTest = getField(ams, "test");
        } catch (Exception e) {
            LOG.warn("Failed to snapshot AMS state for uid={}: {}", getSynopsisID(), e.getMessage());
        }
    }

    private void restoreFromSnapshot() {
        if (snapshotCounts == null || snapshotTest == null) return;
        try {
            setField(ams, "count", snapshotCount);
            setField(ams, "counts", snapshotCounts);
            setField(ams, "test", snapshotTest);
        } catch (Exception e) {
            LOG.warn("Failed to restore AMS state for uid={}: {}", getSynopsisID(), e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object obj, String fieldName) throws Exception {
        Field f = obj.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return (T) f.get(obj);
    }

    private static void setField(Object obj, String fieldName, Object value) throws Exception {
        Field f = obj.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(obj, value);
    }

    @Override
    public void add(JsonNode values) {
        ensureAms();
        String key = values.get(this.keyIndex).asText();
        String value = values.get(this.valueIndex).asText();
        ams.add((long) Math.abs(key.hashCode()), (long) Double.parseDouble(value));
    }

    @Override
    public Object estimate(Object key) {
        ensureAms();
        return ams.estimateCount((long) key);
    }

    @Override
    public Estimation estimate(Request rq) {
        ensureAms();
        try {
            long keyHash = Math.abs(rq.getParam()[0].hashCode());
            String estimation = Double.toString((double) ams.estimateCount(keyHash));
            return new Estimation(rq, estimation, Integer.toString(rq.getSynopsisID()));
        } catch (Exception e) {
            return new Estimation(rq, null, Integer.toString(rq.getSynopsisID()));
        }
    }

    @Override
    public Synopsis merge(Synopsis other) {
        return other;
    }

    // Java serialization support
    private void writeObject(ObjectOutputStream out) throws IOException {
        snapshotState();
        out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // ams will be lazily restored via ensureAms()
    }
}
