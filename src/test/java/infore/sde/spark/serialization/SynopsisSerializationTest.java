package infore.sde.spark.serialization;

import infore.sde.spark.synopses.*;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Critical tests: every Synopsis must survive Java serialization round-trip.
 * This is the #1 risk item — RocksDB state requires Serializable.
 */
class SynopsisSerializationTest {

    @Test
    void countMinSurvivesSerializationRoundTrip() throws Exception {
        String[] params = {"StockID", "price", "Queryable", "0.01", "0.99", "42"};
        CountMin original = new CountMin(1, params);

        CountMin deserialized = serializeAndDeserialize(original);

        assertNotNull(deserialized);
        assertEquals(1, deserialized.getSynopsisID());
        assertEquals("StockID", deserialized.getKeyIndex());
    }

    @Test
    void bloomFilterSurvivesSerializationRoundTrip() throws Exception {
        String[] params = {"StockID", "price", "Queryable", "1000", "0.01"};
        Bloomfilter original = new Bloomfilter(2, params);

        Bloomfilter deserialized = serializeAndDeserialize(original);

        assertNotNull(deserialized);
        assertEquals(2, deserialized.getSynopsisID());
    }

    @Test
    void amsSurvivesSerializationRoundTrip() throws Exception {
        String[] params = {"StockID", "price", "Queryable", "100", "5"};
        AMSsynopsis original = new AMSsynopsis(3, params);

        AMSsynopsis deserialized = serializeAndDeserialize(original);

        assertNotNull(deserialized);
        assertEquals(3, deserialized.getSynopsisID());
    }

    @Test
    void hyperLogLogSurvivesSerializationRoundTrip() throws Exception {
        String[] params = {"StockID", "price", "Queryable", "0.05"};
        HyperLogLogSynopsis original = new HyperLogLogSynopsis(4, params);

        HyperLogLogSynopsis deserialized = serializeAndDeserialize(original);

        assertNotNull(deserialized);
        assertEquals(4, deserialized.getSynopsisID());
    }

    @Test
    void synopsisFactoryCreatesAllTypes() throws Exception {
        assertInstanceOf(CountMin.class,
                SynopsisFactory.create(1, 10, new String[]{"k", "v", "Q", "0.01", "0.99", "1"}));
        assertInstanceOf(Bloomfilter.class,
                SynopsisFactory.create(2, 20, new String[]{"k", "v", "Q", "1000", "0.01"}));
        assertInstanceOf(AMSsynopsis.class,
                SynopsisFactory.create(3, 30, new String[]{"k", "v", "Q", "100", "5"}));
        assertInstanceOf(HyperLogLogSynopsis.class,
                SynopsisFactory.create(4, 40, new String[]{"k", "v", "Q", "0.05"}));
    }

    @Test
    void synopsisFactoryRejectsUnsupportedID() {
        assertThrows(UnsupportedSynopsisException.class,
                () -> SynopsisFactory.create(99, 1, new String[]{}));
    }

    @SuppressWarnings("unchecked")
    private <T extends Serializable> T serializeAndDeserialize(T object) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        try (ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        }
    }
}
