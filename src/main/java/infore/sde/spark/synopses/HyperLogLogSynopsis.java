package infore.sde.spark.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import org.streaminer.stream.cardinality.HyperLogLog;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class HyperLogLogSynopsis extends Synopsis {

    private static final long serialVersionUID = 1L;

    private transient HyperLogLog hll;
    private double relativeStdDev;

    // Byte-array snapshot — Kryo serializes this field directly.
    private byte[] hllBytes;

    /**
     * @param uid        unique synopsis instance ID
     * @param parameters [0]=keyField, [1]=valueField, [2]=operationMode,
     *                   [3]=relativeStdDev (double)
     */
    public HyperLogLogSynopsis(int uid, String[] parameters) {
        super(uid, parameters[0], parameters[1], parameters[2]);
        this.relativeStdDev = Double.parseDouble(parameters[3]);
        this.hll = new HyperLogLog(relativeStdDev);
    }

    private void ensureHll() {
        if (hll == null && hllBytes != null) {
            try {
                hll = HyperLogLog.Builder.build(hllBytes);
            } catch (IOException e) {
                throw new RuntimeException("Failed to restore HyperLogLog from snapshot", e);
            }
        }
    }

    /** Capture current state into byte array before serialization. */
    public void snapshotState() {
        if (hll != null) {
            try {
                hllBytes = hll.getBytes();
            } catch (IOException e) {
                throw new RuntimeException("Failed to snapshot HyperLogLog", e);
            }
        }
    }

    @Override
    public void add(JsonNode values) {
        ensureHll();
        String value = values.get(this.valueIndex).asText();
        hll.offer(value);
    }

    @Override
    public Object estimate(Object key) {
        ensureHll();
        return hll.cardinality();
    }

    @Override
    public Estimation estimate(Request rq) {
        ensureHll();
        String estimation = Double.toString((double) hll.cardinality());
        return new Estimation(rq, estimation, Integer.toString(rq.getUid()));
    }

    @Override
    public Synopsis merge(Synopsis other) {
        return other;
    }

    // Java serialization support (for unit tests and non-Kryo paths)
    private void writeObject(ObjectOutputStream out) throws IOException {
        snapshotState();
        out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // hll will be lazily restored via ensureHll()
    }
}
