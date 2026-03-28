package infore.sde.spark.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Request implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("dataSetkey")
    private String dataSetKey;

    @JsonProperty("requestID")
    private int requestID;

    @JsonProperty("synopsisID")
    private int synopsisID;

    @JsonProperty("uid")
    private int uid;

    @JsonProperty("streamID")
    private String streamID;

    @JsonProperty("param")
    private String[] param;

    @JsonProperty("noOfP")
    private int noOfP;

    public Request() {}

    public Request(String dataSetKey, int requestID, int synopsisID, int uid,
                   String streamID, String[] param, int noOfP) {
        this.dataSetKey = dataSetKey;
        this.requestID = requestID;
        this.synopsisID = synopsisID;
        this.uid = uid;
        this.streamID = streamID;
        this.param = param;
        this.noOfP = noOfP;
    }

    public String getDataSetKey() { return dataSetKey; }
    public void setDataSetKey(String dataSetKey) { this.dataSetKey = dataSetKey; }

    public int getRequestID() { return requestID; }
    public void setRequestID(int requestID) { this.requestID = requestID; }

    public int getSynopsisID() { return synopsisID; }
    public void setSynopsisID(int synopsisID) { this.synopsisID = synopsisID; }

    public int getUid() { return uid; }
    public void setUid(int uid) { this.uid = uid; }

    public String getStreamID() { return streamID; }
    public void setStreamID(String streamID) { this.streamID = streamID; }

    public String[] getParam() { return param; }
    public void setParam(String[] param) { this.param = param; }

    public int getNoOfP() { return noOfP; }
    public void setNoOfP(int noOfP) { this.noOfP = noOfP; }

    @Override
    public String toString() {
        return "Request{dataSetKey='" + dataSetKey + "', requestID=" + requestID +
               ", synopsisID=" + synopsisID + ", uid=" + uid +
               ", streamID='" + streamID + "', param=" + Arrays.toString(param) +
               ", noOfP=" + noOfP + "}";
    }
}
