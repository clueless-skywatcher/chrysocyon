package com.github.cluelessskywatcher.chrysocyon.tuples.data;

import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;
import com.github.cluelessskywatcher.chrysocyon.tuples.exceptions.IncomparableDataFieldException;

import lombok.Getter;

public class VarStringField implements DataField {
    private String value;
    private @Getter int maxSize;

    public VarStringField(String value, int maxSize) {
        this.value = value;
        this.maxSize = maxSize;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public int getSize() {
        return PageObject.maxStringLength(this.value.length());
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other instanceof VarStringField) {
            return this.value.equals(((VarStringField)other).value);
        }
        return false;
    }

    @Override
    public int compareTo(DataField o) {
        if (o instanceof VarStringField) {
            return Integer.compare((int) this.getValue(), (int) o.getValue());
        }
        throw new IncomparableDataFieldException(String.format("Cannot compare %s to %s", toString(), o.toString()));
    }

    public int hashCode() {
        return this.getValue().hashCode();
    }

    public String toString() {
        return (String) this.getValue();
    }
}
