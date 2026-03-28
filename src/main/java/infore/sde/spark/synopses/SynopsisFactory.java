package infore.sde.spark.synopses;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynopsisFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SynopsisFactory.class);

    private SynopsisFactory() {}

    /**
     * Creates a synopsis instance based on the synopsisID.
     *
     * @param synopsisID algorithm type (1=CountMin, 2=Bloom, 3=AMS, 4=HLL)
     * @param uid        unique synopsis instance identifier
     * @param params     algorithm-specific parameters
     * @return new Synopsis instance
     * @throws UnsupportedSynopsisException if synopsisID is not supported
     */
    // Minimum param[] lengths per synopsis type:
    // All require [0]=keyField, [1]=valueField, [2]=operationMode, plus type-specific params
    private static final int[] MIN_PARAMS = {0, 6, 5, 5, 4}; // index = synopsisID

    public static Synopsis create(int synopsisID, int uid, String[] params)
            throws UnsupportedSynopsisException {
        if (params == null) {
            throw new IllegalArgumentException("params cannot be null for uid=" + uid);
        }
        if (synopsisID < 1 || synopsisID > 4) {
            throw new UnsupportedSynopsisException(synopsisID);
        }
        if (params.length < MIN_PARAMS[synopsisID]) {
            throw new IllegalArgumentException(String.format(
                    "Synopsis %d (uid=%d) requires at least %d params, got %d",
                    synopsisID, uid, MIN_PARAMS[synopsisID], params.length));
        }

        return switch (synopsisID) {
            case 1 -> {
                LOG.info("Creating CountMin sketch uid={}", uid);
                yield new CountMin(uid, params);
            }
            case 2 -> {
                LOG.info("Creating BloomFilter uid={}", uid);
                yield new Bloomfilter(uid, params);
            }
            case 3 -> {
                LOG.info("Creating AMS sketch uid={}", uid);
                yield new AMSsynopsis(uid, params);
            }
            case 4 -> {
                LOG.info("Creating HyperLogLog uid={}", uid);
                yield new HyperLogLogSynopsis(uid, params);
            }
            default -> throw new UnsupportedSynopsisException(synopsisID);
        };
    }
}
