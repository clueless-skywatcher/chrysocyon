package com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params;

import lombok.Getter;

public class IndexCatalogParameters extends AbstractMetaTableParameters {
    private @Getter String indexName;
    private @Getter String tableName;
    private @Getter String fieldName;
    
    public IndexCatalogParameters(String indexName, String tableName, String fieldName) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.fieldName = fieldName;
    }
}
