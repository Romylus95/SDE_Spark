package infore.sde.spark.synopses;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.fasterxml.jackson.databind.JsonNode;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Bloomfilter extends Synopsis {

    private static final long serialVersionUID = 1L;

    private transient BloomFilter bloomFilter;
    private int expectedInsertions;
    private double falsePositiveRate;

    // Byte-array snapshot — Kryo serializes this field directly.
    // Kept in sync via snapshotState() / restoreState().
    private byte[] bloomFilterBytes;

    /**
     * @param uid        unique synopsis instance ID
     * @param parameters [0]=keyField, [1]=valueField, [2]=operationMode,
     *                   [3]=expectedInsertions (int), [4]=falsePositiveRate (double)
     */
    public Bloomfilter(int uid, String[] parameters) {
        super(uid, parameters[0], parameters[1], parameters[2]);
        this.expectedInsertions = Integer.parseInt(parameters[3]);
        this.falsePositiveRate = Double.parseDouble(parameters[4]);
        this.bloomFilter = new BloomFilter(expectedInsertions, falsePositiveRate);
    }

    private void ensureBloomFilter() {
        if (bloomFilter == null && bloomFilterBytes != null) {
            bloomFilter = BloomFilter.deserialize(bloomFilterBytes);
        }
    }

    /** Capture current state into byte array before serialization. */
    public void snapshotState() {
        if (bloomFilter != null) {
            bloomFilterBytes = BloomFilter.serialize(bloomFilter);
        }
    }

    @Override
    public void add(JsonNode values) {
        ensureBloomFilter();
        String key = values.get(this.keyIndex).asText();
        bloomFilter.add(key);
    }

    @Override
    public Object estimate(Object key) {
        ensureBloomFilter();
        return bloomFilter.isPresent((String) key);
    }

    @Override
    public Estimation estimate(Request rq) {
        ensureBloomFilter();
        boolean present = bloomFilter.isPresent(rq.getParam()[0]);
        return new Estimation(rq, present, Integer.toString(rq.getUid()));
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
        // bloomFilter will be lazily restored via ensureBloomFilter()
    }
}
