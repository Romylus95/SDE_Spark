package infore.sde.spark.routing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * State for the DataRouter. Tracks which parallelism levels are registered
 * and what routing keys exist per stream.
 */
public class RoutingState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** parallelismLevel -> count of synopses using that level */
    private final Map<Integer, Integer> keyedParallelism = new HashMap<>();

    /** streamID -> list of routing keys for that stream */
    private final Map<String, List<String>> keysPerStream = new HashMap<>();

    /** uid -> original request (for cleanup on DELETE) */
    private final Map<Integer, RoutingRegistration> registrations = new HashMap<>();

    public Map<Integer, Integer> getKeyedParallelism() { return keyedParallelism; }
    public Map<String, List<String>> getKeysPerStream() { return keysPerStream; }
    public Map<Integer, RoutingRegistration> getRegistrations() { return registrations; }

    public static class RoutingRegistration implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int requestID;
        private final int noOfP;
        private final String dataSetKey;

        public RoutingRegistration(int requestID, int noOfP, String dataSetKey) {
            this.requestID = requestID;
            this.noOfP = noOfP;
            this.dataSetKey = dataSetKey;
        }

        public int getRequestID() { return requestID; }
        public int getNoOfP() { return noOfP; }
        public String getDataSetKey() { return dataSetKey; }
    }
}
