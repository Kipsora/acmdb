package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByField;
    private final int field;
    private final TupleDesc tupleDesc;

    private final Map<Field, Tuple> groupTuple;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here

        this.groupByField = gbfield;
        this.field = afield;
        if (!Objects.equals(what, Op.COUNT)) {
            throw new IllegalArgumentException("StringAggregator only supports count operator");
        }
        if (this.groupByField == NO_GROUPING) {
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        this.groupTuple = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tuple the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tuple) {
        // some code goes here
        assert tuple.getField(field) instanceof StringField;
        Field key = groupByField == NO_GROUPING ? null : tuple.getField(groupByField);
        String value = ((StringField) tuple.getField(field)).getValue();

        Tuple oldTuple = groupTuple.getOrDefault(key, null);
        int oldValue = oldTuple == null ? 0 : ((IntField) oldTuple.getField(key == null ? 0 : 1)).getValue();

        Tuple newTuple = oldTuple == null ? new Tuple(tupleDesc) : oldTuple;
        if (key != null) {
            newTuple.setField(0, key);
            newTuple.setField(1, new IntField(oldValue + 1));
        } else {
            newTuple.setField(0, new IntField(oldValue + 1));
        }
        groupTuple.put(key, newTuple);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new TupleIterator(tupleDesc, groupTuple.values());
    }

}
