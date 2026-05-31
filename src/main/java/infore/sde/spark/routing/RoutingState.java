package infore.sde.spark.routing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * State for the DataRouter. Tracks synopsis registrations keyed by uid.
 */
public class RoutingState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** uid -> registration (noOfP, requestID, dataSetKey) */
    private final Map<Integer, RoutingRegistration> registrations = new HashMap<>();

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
