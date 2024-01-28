package com.github.cluelessskywatcher.chrysocyon.metadata;

import lombok.Getter;

public class StatisticalInfo {
    private @Getter int numBlocksAccessed;
    private @Getter int numRecords;
    private @Getter int distinctValues;

    public StatisticalInfo(int numBlocksAccessed, int numRecords) {
        this.numBlocksAccessed = numBlocksAccessed;
        this.numRecords = numRecords;
        this.distinctValues = 1 + (numRecords / 3);
    }

    public StatisticalInfo(int numBlocksAccessed, int numRecords, int distinctValues) {
        this.numBlocksAccessed = numBlocksAccessed;
        this.numRecords = numRecords;
        this.distinctValues = distinctValues;
    }
}
