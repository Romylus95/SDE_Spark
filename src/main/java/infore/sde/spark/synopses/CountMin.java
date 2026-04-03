package infore.sde.spark.synopses;

import com.fasterxml.jackson.databind.JsonNode;
import infore.sde.spark.messages.Estimation;
import infore.sde.spark.messages.Request;
import infore.sde.spark.synopses.sketches.CM;

/**
 * CountMin Sketch synopsis — frequency estimation.
 * Estimates the total accumulated value for a given key (e.g., total price per stock).
 * Uses a custom CM sketch implementation with configurable epsilon and delta.
 *
 * Unlike other synopsis types, CountMin does not require the snapshot/restore pattern
 * because the CM class is fully Serializable (no transient fields).
 */
public class CountMin extends Synopsis {

    private static final long serialVersionUID = 1L;

    private final CM cm;
    private int count = 0;

    /**
     * @param uid        unique synopsis instance ID
     * @param parameters [0]=keyField, [1]=valueField, [2]=operationMode,
     *                   [3]=epsilon (double), [4]=delta (double), [5]=seed (int)
     */
    public CountMin(int uid, String[] parameters) {
        super(uid, parameters[0], parameters[1], parameters[2]);
        this.cm = new CM(
                Double.parseDouble(parameters[3]),
                Double.parseDouble(parameters[4]),
                Integer.parseInt(parameters[5])
        );
    }

    @Override
    public void add(JsonNode values) {
        count++;
        String key = values.get(this.keyIndex).asText();

        if (this.valueIndex.startsWith("null")) {
            cm.add(key, 1);
        } else {
            String value = values.get(this.valueIndex).asText();
            cm.add(key, (long) Double.parseDouble(value));
        }
    }

    @Override
    public Object estimate(Object key) {
        return cm.estimateCount((String) key);
    }

    @Override
    public Estimation estimate(Request rq) {
        String key = rq.getParam()[0];
        String estimation = Double.toString((double) cm.estimateCount(key));
        return new Estimation(rq, estimation, Integer.toString(rq.getUid()));
    }

    @Override
    public Synopsis merge(Synopsis other) {
        // For Purple path aggregation, the reduce function handles merging
        // via SimpleSumFunction (summing the estimated counts)
        return other;
    }

    public int getCount() { return count; }
}
