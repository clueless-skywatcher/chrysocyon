package com.github.cluelessskywatcher.chrysocyon.processing.expressions;

import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class PredExpression {
    private DataField value = null;
    private String fieldName = null;

    public PredExpression(DataField value) {
        this.value = value;
    }

    public PredExpression(String fieldName) {
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
        if (o instanceof PredExpression) {
            PredExpression other = (PredExpression) o;
            return (value != null) ? value.equals(other.value) : fieldName.equals(other.fieldName);
        }
        return false;
    }
}
