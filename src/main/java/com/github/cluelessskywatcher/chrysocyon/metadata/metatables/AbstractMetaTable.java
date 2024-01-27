package com.github.cluelessskywatcher.chrysocyon.metadata.metatables;

import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

import lombok.Getter;

public abstract class AbstractMetaTable {
    protected @Getter String tableName;
    protected @Getter TupleSchema schema;
    protected @Getter TupleLayout layout;
    

    public AbstractMetaTable() {
        initMetaTable();
    }

    public abstract void initMetaTable();

    public abstract void insertData(ChrysoTransaction txn, AbstractMetaTableParameters params);

    public int getTypeCode(String fieldName) {
        return schema.getType(fieldName).getCode();
    }

    public int getOffset(String fieldName) {
        return layout.getOffset(fieldName);
    }
}
