package infore.sde.spark.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Datapoint implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("dataSetkey")
    private String dataSetKey;

    @JsonProperty("streamID")
    private String streamID;

    @JsonProperty("values")
    private JsonNode values;

    public Datapoint() {}

    public Datapoint(String dataSetKey, String streamID, JsonNode values) {
        this.dataSetKey = dataSetKey;
        this.streamID = streamID;
        this.values = values;
    }

    public String getDataSetKey() { return dataSetKey; }
    public void setDataSetKey(String dataSetKey) { this.dataSetKey = dataSetKey; }

    public String getStreamID() { return streamID; }
    public void setStreamID(String streamID) { this.streamID = streamID; }

    public JsonNode getValues() { return values; }
    public void setValues(JsonNode values) { this.values = values; }

    @Override
    public String toString() {
        return "Datapoint{dataSetKey='" + dataSetKey + "', streamID='" + streamID + "', values=" + values + "}";
    }
}
