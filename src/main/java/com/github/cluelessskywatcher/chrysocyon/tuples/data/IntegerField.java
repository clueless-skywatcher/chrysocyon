package com.github.cluelessskywatcher.chrysocyon.tuples.data;

import com.github.cluelessskywatcher.chrysocyon.tuples.exceptions.IncomparableDataFieldException;

public class IntegerField implements DataField  {
    private int value;

    public IntegerField(int value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public int getSize() {
        return Integer.BYTES;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other instanceof IntegerField) {
            return this.value == ((IntegerField)other).value;
        }
        return false;
    }

    @Override
    public int compareTo(DataField o) {
        if (o instanceof IntegerField) {
            return Integer.compare((int) this.getValue(), (int) o.getValue());
        }
        throw new IncomparableDataFieldException(String.format("Cannot compare %s to %s", toString(), o.toString()));
    }

    public int hashCode() {
        return this.getValue().hashCode();
    }

    public String toString() {
        return this.getValue().toString();
    }
}
