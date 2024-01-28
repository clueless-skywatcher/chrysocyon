package com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params;

import lombok.Getter;

public class TableStatisticsParameters extends AbstractMetaTableParameters {
    private @Getter String tableName;
    private @Getter int numBlocks;
    private @Getter int numRecords;

    public TableStatisticsParameters(String tableName, int numBlocks, int numRecords) {
        this.tableName = tableName;
        this.numBlocks = numBlocks;
        this.numRecords = numRecords;
    }
}
