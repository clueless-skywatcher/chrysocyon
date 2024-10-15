package com.github.cluelessskywatcher.chrysocyon.tuples.data;

public interface DataField extends Comparable<DataField> {
    public Object getValue();

    public int getSize();

    public boolean equals(Object other);
}
