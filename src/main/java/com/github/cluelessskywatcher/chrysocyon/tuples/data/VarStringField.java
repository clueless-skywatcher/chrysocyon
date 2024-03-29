package com.github.cluelessskywatcher.chrysocyon.tuples.data;

import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

import lombok.Getter;

public class VarStringField implements DataField, Comparable<VarStringField> {
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
    public int compareTo(VarStringField o) {
        return ((String) getValue()).compareTo((String) o.getValue());
    }

    public int hashCode() {
        return this.getValue().hashCode();
    }

    public String toString() {
        return (String) this.getValue();
    }
}
