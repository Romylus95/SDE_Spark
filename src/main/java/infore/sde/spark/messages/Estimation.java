package infore.sde.spark.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Estimation implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("key")
    private String key;

    @JsonProperty("estimationkey")
    private String estimationKey;

    @JsonProperty("streamID")
    private String streamID;

    @JsonProperty("uid")
    private int uid;

    @JsonProperty("requestID")
    private int requestID;

    @JsonProperty("synopsisID")
    private int synopsisID;

    @JsonProperty("estimation")
    private Object estimation;

    @JsonProperty("param")
    private String[] param;

    @JsonProperty("noOfP")
    private int noOfP;

    public Estimation() {}

    public Estimation(int uid, String estimationKey, int requestID, int synopsisID,
                      String key, Object estimation, String[] param, int noOfP) {
        this.uid = uid;
        this.estimationKey = estimationKey;
        this.requestID = requestID;
        this.synopsisID = synopsisID;
        this.key = key;
        this.estimation = estimation;
        this.param = param;
        this.noOfP = noOfP;
    }

    public Estimation(Request rq, Object estimation, String estimationKey) {
        this.uid = rq.getUid();
        this.requestID = rq.getRequestID();
        this.synopsisID = rq.getSynopsisID();
        this.key = rq.getDataSetKey();
        this.streamID = rq.getStreamID();
        this.estimation = estimation;
        this.estimationKey = estimationKey;
        this.param = rq.getParam();
        this.noOfP = rq.getNoOfP();
    }

    public Estimation(Estimation other) {
        this.key = other.key;
        this.estimationKey = other.estimationKey;
        this.streamID = other.streamID;
        this.uid = other.uid;
        this.requestID = other.requestID;
        this.synopsisID = other.synopsisID;
        this.estimation = other.estimation;
        this.param = other.param != null ? other.param.clone() : null;
        this.noOfP = other.noOfP;
    }

    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }

    public String getEstimationKey() { return estimationKey; }
    public void setEstimationKey(String estimationKey) { this.estimationKey = estimationKey; }

    public String getStreamID() { return streamID; }
    public void setStreamID(String streamID) { this.streamID = streamID; }

    public int getUid() { return uid; }
    public void setUid(int uid) { this.uid = uid; }

    public int getRequestID() { return requestID; }
    public void setRequestID(int requestID) { this.requestID = requestID; }

    public int getSynopsisID() { return synopsisID; }
    public void setSynopsisID(int synopsisID) { this.synopsisID = synopsisID; }

    public Object getEstimation() { return estimation; }
    public void setEstimation(Object estimation) { this.estimation = estimation; }

    public String[] getParam() { return param; }
    public void setParam(String[] param) { this.param = param; }

    public int getNoOfP() { return noOfP; }
    public void setNoOfP(int noOfP) { this.noOfP = noOfP; }

    @Override
    public String toString() {
        return "Estimation{key='" + key + "', estimationKey='" + estimationKey +
               "', uid=" + uid + ", requestID=" + requestID +
               ", synopsisID=" + synopsisID + ", estimation=" + estimation +
               ", noOfP=" + noOfP + ", param=" + Arrays.toString(param) + "}";
    }
}
