package com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params;

import lombok.Getter;

public class FieldCatalogParameters extends AbstractMetaTableParameters {
    private @Getter String tableName;
    private @Getter String fieldName;
    private @Getter int type;
    private @Getter int offset;
    private @Getter int length;

    public FieldCatalogParameters(String tableName, String fieldName, int type, int offset, int length) {
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.type = type;
        this.offset = offset;
        this.length = length;
    }
}
