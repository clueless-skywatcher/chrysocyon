package com.github.cluelessskywatcher.chrysocyon.metadata;

import lombok.Getter;

public class StatisticalInfo {
    private @Getter int numBlocksAccessed;
    private @Getter int numRecords;
    
    public StatisticalInfo(int numBlocksAccessed, int numRecords) {
        this.numBlocksAccessed = numBlocksAccessed;
        this.numRecords = numRecords;
    }

    public StatisticalInfo(int numBlocksAccessed, int numRecords, int distinctValues) {
        this.numBlocksAccessed = numBlocksAccessed;
        this.numRecords = numRecords;
    }

    public int getDistinctValues(String fieldName) {
        return numRecords == 0 ? 1 : 1 + (numRecords / 3);
    }
}
