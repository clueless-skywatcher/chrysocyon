package com.github.cluelessskywatcher.chrysocyon.tuples;

import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;

import lombok.Getter;

public class TupleIdentifier {
    private @Getter int slot;
    private @Getter int block;

    public TupleIdentifier(int blockNumber, int slot) {
        this.block = blockNumber;
        this.slot = slot;
    }

    public boolean equals(Object other) {
        if (other == null) return false;
        if (other instanceof TupleIdentifier) {
            return this.block == ((TupleIdentifier)other).block
                && this.slot == ((TupleIdentifier)other).slot;
        }
        return false;
    }
}
