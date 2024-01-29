package com.github.cluelessskywatcher.chrysocyon.metadata.metatables;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.AbstractMetaTableParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.SchemaCatalogParameters;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

public class SchemaCatalogTable extends AbstractMetaTable {
    private static final int MAX_TABLE_NAME_SIZE = 20;
    
    public SchemaCatalogTable() {
        super();
    }

    @Override
    public void initMetaTable() {
        tableName = "schema_catalog";
        schema = new TupleSchema();
        schema.addField(new VarStringInfo(MAX_TABLE_NAME_SIZE), "table_name");
        schema.addField(new IntegerInfo(), "slot_size");
        layout = new TupleLayout(schema);
    }

    @Override
    public void insertData(ChrysoTransaction txn, AbstractMetaTableParameters params) {
        TableScan scan = new TableScan(txn, tableName, layout);
        SchemaCatalogParameters scParams = (SchemaCatalogParameters) params;
        scan.insert();
        scan.setData("table_name", new VarStringField(scParams.getTableName(), MAX_TABLE_NAME_SIZE));
        scan.setData("slot_size", new IntegerField(scParams.getLayout().getSlotSize()));
        scan.close();
    }
}
