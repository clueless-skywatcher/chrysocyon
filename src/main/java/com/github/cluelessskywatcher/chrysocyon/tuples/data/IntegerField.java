package com.github.cluelessskywatcher.chrysocyon.tuples.data;

public class IntegerField implements DataField {
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
}
