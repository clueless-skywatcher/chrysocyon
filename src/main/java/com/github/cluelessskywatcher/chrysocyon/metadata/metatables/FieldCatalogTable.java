package com.github.cluelessskywatcher.chrysocyon.metadata.metatables;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.AbstractMetaTableParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.FieldCatalogParameters;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

public class FieldCatalogTable extends AbstractMetaTable {
    private static final int MAX_TABLE_NAME_SIZE = 20;
    private static final int MAX_FIELD_NAME_SIZE = 20;

    public FieldCatalogTable() {
        super();    
    }

    @Override
    public void initMetaTable() {
        tableName = "field_catalog";
        schema = new TupleSchema();
        schema.addField(new VarStringInfo(MAX_TABLE_NAME_SIZE), "table_name");
        schema.addField(new VarStringInfo(MAX_FIELD_NAME_SIZE), "field_name");
        schema.addField(new IntegerInfo(), "type");
        schema.addField(new IntegerInfo(), "length");
        schema.addField(new IntegerInfo(), "offset");
        layout = new TupleLayout(schema);
    }

    @Override
    public void insertData(ChrysoTransaction txn, AbstractMetaTableParameters params) {
        TableScan scan = new TableScan(txn, tableName, layout);
        FieldCatalogParameters fcParams = (FieldCatalogParameters) params;
        scan.insert();
        scan.setData("table_name", new VarStringField(fcParams.getTableName(), MAX_TABLE_NAME_SIZE));
        scan.setData("field_name", new VarStringField(fcParams.getFieldName(), MAX_FIELD_NAME_SIZE));
        scan.setData("type", new IntegerField(fcParams.getType()));
        scan.setData("length", new IntegerField(fcParams.getLength()));
        scan.setData("offset", new IntegerField(fcParams.getOffset()));
        scan.close();
    }
}
