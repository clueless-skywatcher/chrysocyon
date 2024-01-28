package com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params;

import lombok.Getter;

public class FieldStatisticsParameters extends AbstractMetaTableParameters {
    private @Getter String tableName;
    private @Getter String fieldName;
    private @Getter int numValues;
    
    public FieldStatisticsParameters(String tableName, String fieldName, int numValues) {
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.numValues = numValues;
    }   
}
