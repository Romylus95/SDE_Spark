package infore.sde.spark.synopses.sketches;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import java.util.Arrays;
import java.util.Random;

import com.clearspring.analytics.stream.membership.Filter;
import com.clearspring.analytics.util.Preconditions;

/**
 * Count-Min Sketch data structure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 *
 * Ported from infore.SDE.synopses.Sketches.CM (Flink project).
 */
public class CM implements Serializable {

    public static final long PRIME_MODULUS = (1L << 31) - 1;
    private static final long serialVersionUID = -5084982213094657923L;

    int depth;
    int width;
    long[][] table;
    long[] hashA;
    long size;
    double eps;
    double confidence;

    CM() {}

    public CM(int depth, int width, int seed) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        initTablesWith(depth, width, seed);
    }

    public CM(double epsOfTotalCount, double confidence, int seed) {
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(2 / epsOfTotalCount);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        initTablesWith(depth, width, seed);
    }

    CM(int depth, int width, long size, long[] hashA, long[][] table) {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.hashA = hashA;
        this.table = table;
        Preconditions.checkState(size >= 0, "The size cannot be smaller than ZERO: " + size);
        this.size = size;
    }

    private void initTablesWith(int depth, int width, int seed) {
        this.table = new long[depth][width];
        this.hashA = new long[depth];
        Random r = new Random(seed);
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }

    public double getRelativeError() { return eps; }
    public double getConfidence() { return confidence; }
    public int getDepth() { return depth; }
    public int getWidth() { return width; }
    public long[][] getTable() { return table; }
    public long size() { return size; }

    int hash(long item, int i) {
        long hash = hashA[i] * item;
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        return ((int) hash) % width;
    }

    private static void checkSizeAfterOperation(long previousSize, String operation, long newSize) {
        if (newSize < previousSize) {
            throw new IllegalStateException("Overflow error: the size after calling `" + operation +
                    "` is smaller than the previous size. " +
                    "Previous size: " + previousSize + ", New size: " + newSize);
        }
    }

    private void checkSizeAfterAdd(String item, long count) {
        long previousSize = size;
        size += count;
        checkSizeAfterOperation(previousSize, "add(" + item + "," + count + ")", size);
    }

    public void add(long item, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        for (int i = 0; i < depth; ++i) {
            table[i][hash(item, i)] += count;
        }
        checkSizeAfterAdd(String.valueOf(item), count);
    }

    public void add(String item, long count) {
        if (count < 0) {
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            table[i][buckets[i]] += count;
        }
        checkSizeAfterAdd(item, count);
    }

    public long estimateCount(long item) {
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    public long estimateCount(String item) {
        long res = Long.MAX_VALUE;
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][buckets[i]]);
        }
        return res;
    }

    public void merge(CM estimator) {
        if (estimator != null) {
            if (this.depth != estimator.depth) return;
            if (this.width != estimator.width) return;
            if (!Arrays.equals(this.hashA, estimator.hashA)) return;

            for (int i = 0; i < this.table.length; i++) {
                for (int j = 0; j < this.table[i].length; j++) {
                    this.table[i][j] += estimator.table[i][j];
                }
            }
            long previousSize = size;
            size += estimator.size;
            checkSizeAfterOperation(previousSize, "merge(" + estimator + ")", size);
        }
    }

    public static byte[] serialize(CM sketch) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream s = new DataOutputStream(bos);
        try {
            s.writeLong(sketch.size);
            s.writeInt(sketch.depth);
            s.writeInt(sketch.width);
            for (int i = 0; i < sketch.depth; ++i) {
                s.writeLong(sketch.hashA[i]);
                for (int j = 0; j < sketch.width; ++j) {
                    s.writeLong(sketch.table[i][j]);
                }
            }
            s.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static CM deserialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream s = new DataInputStream(bis);
        try {
            CM sketch = new CM();
            sketch.size = s.readLong();
            sketch.depth = s.readInt();
            sketch.width = s.readInt();
            sketch.eps = 2.0 / sketch.width;
            sketch.confidence = 1 - 1 / Math.pow(2, sketch.depth);
            sketch.hashA = new long[sketch.depth];
            sketch.table = new long[sketch.depth][sketch.width];
            for (int i = 0; i < sketch.depth; ++i) {
                sketch.hashA[i] = s.readLong();
                for (int j = 0; j < sketch.width; ++j) {
                    sketch.table[i][j] = s.readLong();
                }
            }
            return sketch;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "CM{eps=" + eps + ", confidence=" + confidence +
                ", depth=" + depth + ", width=" + width + ", size=" + size + "}";
    }
}
