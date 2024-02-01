package com.github.cluelessskywatcher.chrysocyon.processing.expressions;

import com.github.cluelessskywatcher.chrysocyon.planning.OpPlan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class PredTerm {
    private PredExpression l;
    private PredExpression r;
    private ExpressionOperator op;
    
    public PredTerm(PredExpression l, PredExpression r) {
        this.l = l;
        this.r = r;
        this.op = ExpressionOperator.EQUALS;
    }

    public PredTerm(PredExpression l, PredExpression r, ExpressionOperator op) {
        this(l, r);
        this.op = op;
    }

    public boolean isSatisfied(IScan s) {
        DataField lVal = l.evaluateScan(s);
        DataField rVal = r.evaluateScan(s);

        switch (op) {
            case EQUALS:
                return lVal.equals(rVal);
            default:
                return false;
        }
    }

    public DataField equatesWithConstant(String field) {
        if (l.isFieldName() && l.asFieldName().equals(field) && !r.isFieldName()) {
            return r.asValue();
        }
        else if (r.isFieldName() && r.asFieldName().equals(field) && !l.isFieldName())
            return l.asValue();
        return null;
    }

    public String equatesWithField(String field) {
        if (l.isFieldName() && l.asFieldName().equals(field) && r.isFieldName()) {
            return r.asFieldName();
        }
        else if (r.isFieldName() && r.asFieldName().equals(field) && l.isFieldName())
            return l.asFieldName();
        return null;
    }

    public String toString() {
        return String.format("%s %s %s", l.toString(), op.toString(), r.toString());
    }

    public boolean appliesTo(TupleSchema schema) {
        return l.appliesTo(schema) && r.appliesTo(schema);
    }

    public boolean equals(Object o) {
        if (o instanceof PredTerm) {
            PredTerm other = (PredTerm) o;
            return l.equals(other.l) && r.equals(other.r) && op.equals(other.op);
        }
        return false;
    }

    public int reductionFactor(OpPlan p) {
        String lhsName, rhsName;
        if (l.isFieldName() && r.isFieldName()) {
            lhsName = l.asFieldName();
            rhsName = r.asFieldName();
            return Math.max(p.distinctValues(lhsName), p.distinctValues(rhsName));
        }
        if (l.isFieldName()) {
            lhsName = l.asFieldName();
            return p.distinctValues(lhsName);
        }
        if (r.isFieldName()) {
            rhsName = r.asFieldName();
            return p.distinctValues(rhsName);
        }
        if (l.asValue().equals(r.asValue())) {
            return 1;
        }
        else {
            return Integer.MAX_VALUE;
        }
    }
}
