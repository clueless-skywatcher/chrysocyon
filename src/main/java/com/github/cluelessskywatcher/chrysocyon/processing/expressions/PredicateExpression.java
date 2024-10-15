package com.github.cluelessskywatcher.chrysocyon.processing.expressions;

import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class PredicateExpression {
    private DataField value = null;
    private String fieldName = null;

    public PredicateExpression(DataField value) {
        this.value = value;
    }

    public PredicateExpression(String fieldName) {
        this.fieldName = fieldName;
    }

    public boolean isFieldName() {
        return this.fieldName != null;
    }

    public DataField asValue() {
        return this.value;
    }

    public String asFieldName() {
        return this.fieldName;
    }

    public DataField evaluateScan(IScan scan) {
        return (this.value != null) ? this.value : scan.getData(fieldName);
    }

    public boolean appliesTo(TupleSchema schema) {
        return (this.value != null) ? true : schema.hasField(fieldName);
    }

    public String toString() {
        return (this.value != null) ? value.toString() : fieldName;
    }

    public boolean equals(Object o) {
        if (o instanceof PredicateExpression) {
            PredicateExpression other = (PredicateExpression) o;
            return (value != null) ? value.equals(other.value) : fieldName.equals(other.fieldName);
        }
        return false;
    }
}
