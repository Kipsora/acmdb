package simpledb;

import java.io.Serializable;
import java.util.Objects;

/**
 * IndexPredicate compares a field which has index on it against a given value
 * @see simpledb.IndexDbIterator
 */
public class IndexPredicate implements Serializable {
	
    private static final long serialVersionUID = 1L;

    private Predicate.Op op;
    private Field fvalue;
	
    /**
     * Constructor.
     *
     * @param fvalue The value that the predicate compares against.
     * @param op The operation to apply (as defined in Predicate.Op); either
     *   Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN, Predicate.Op.EQUAL,
     *   Predicate.Op.GREATER_THAN_OR_EQ, or Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public IndexPredicate(Predicate.Op op, Field fvalue) {
        // some code goes here
        this.op = op;
        this.fvalue = fvalue;
    }

    public Field getField() {
        // some code goes here
        return fvalue;
    }

    public Predicate.Op getOp() {
        // some code goes here
        return op;
    }

    /** Return true if the fieldvalue in the supplied predicate
        is satisfied by this predicate's fieldvalue and
        operator.
        @param ipd The field to compare against.
    */
    public boolean equals(IndexPredicate ipd) {
        // some code goes here
        return Objects.equals(op, ipd.op) && Objects.equals(fvalue, ipd.fvalue);
    }

}
