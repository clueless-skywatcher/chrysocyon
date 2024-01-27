package com.github.cluelessskywatcher.chrysocyon.metadata.metatables;

import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;

import lombok.Getter;
import lombok.Setter;

public class SchemaCatalogParameters extends AbstractMetaTableParameters {
    private @Getter @Setter String tableName;
    private @Getter @Setter TupleLayout layout;

    public SchemaCatalogParameters(String tableName, TupleLayout layout) {
        this.tableName = tableName;
        this.layout = layout;
    }
}
