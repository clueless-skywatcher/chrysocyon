package com.github.cluelessskywatcher.chrysocyon.metadata.metatables;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.FieldConstants;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.IndexConstants;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.TableConstants;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.AbstractMetaTableParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.IndexCatalogParameters;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

public class IndexCatalogTable extends AbstractMetaTable {
    public IndexCatalogTable() {
        super();
    }
    @Override
    public void initMetaTable() {
        tableName = "index_catalog";
        schema = new TupleSchema();
        schema.addField(new VarStringInfo(IndexConstants.MAX_INDEX_NAME_SIZE), "index_name");
        schema.addField(new VarStringInfo(TableConstants.MAX_TABLE_NAME_SIZE), "table_name");
        schema.addField(new VarStringInfo(FieldConstants.MAX_FIELD_NAME_SIZE), "field_name");
        layout = new TupleLayout(schema);
    }

    @Override
    public void insertData(ChrysoTransaction txn, AbstractMetaTableParameters params) {
        IndexCatalogParameters iParams = (IndexCatalogParameters) params;

        TableScan iScan = new TableScan(txn, tableName, layout);
        iScan.insert();
        iScan.setData("index_name", new VarStringField(iParams.getIndexName(), IndexConstants.MAX_INDEX_NAME_SIZE));
        iScan.setData("table_name", new VarStringField(iParams.getTableName(), TableConstants.MAX_TABLE_NAME_SIZE));
        iScan.setData("field_name", new VarStringField(iParams.getFieldName(), FieldConstants.MAX_FIELD_NAME_SIZE));
        iScan.close();
    }
    
}
