package com.github.cluelessskywatcher.chrysocyon.index.btree;

import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class BTreeDirectoryEntry {
    private @Getter DataField dataValue;
    private @Getter int blockNumber;

    public BTreeDirectoryEntry(DataField dataValue, int blockNumber) {
        this.dataValue = dataValue;
        this.blockNumber = blockNumber;
    }
}
