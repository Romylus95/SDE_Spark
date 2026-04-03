package infore.sde.spark.processing;

import infore.sde.spark.messages.Datapoint;
import infore.sde.spark.messages.Request;

import java.io.Serializable;

/**
 * Union discriminator type — the key design pattern in SDE_Spark.
 *
 * Spark Structured Streaming does not support two-input streams natively.
 * InputEvent wraps either a Datapoint (DATA) or a Request (REQUEST) so that
 * both streams can be merged into a single Dataset<InputEvent> via union(),
 * then dispatched inside flatMapGroupsWithState based on the type field.
 *
 * Each InputEvent carries a dataSetKey extracted from the wrapped message,
 * used as the groupByKey partition key for routing and processing.
 */
public class InputEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Type { DATA, REQUEST }

    private final Type type;
    private final Datapoint datapoint;
    private final Request request;
    private final String dataSetKey;

    private InputEvent(Type type, Datapoint datapoint, Request request, String dataSetKey) {
        this.type = type;
        this.datapoint = datapoint;
        this.request = request;
        this.dataSetKey = dataSetKey;
    }

    public static InputEvent data(Datapoint dp) {
        return new InputEvent(Type.DATA, dp, null, dp.getDataSetKey());
    }

    public static InputEvent request(Request rq) {
        return new InputEvent(Type.REQUEST, null, rq, rq.getDataSetKey());
    }

    public Type getType() { return type; }
    public Datapoint getDatapoint() { return datapoint; }
    public Request getRequest() { return request; }
    public String getDataSetKey() { return dataSetKey; }

    public boolean isData() { return type == Type.DATA; }
    public boolean isRequest() { return type == Type.REQUEST; }
}
