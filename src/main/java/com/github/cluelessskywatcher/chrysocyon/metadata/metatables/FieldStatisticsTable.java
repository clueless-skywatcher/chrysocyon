package com.github.cluelessskywatcher.chrysocyon.metadata.metatables;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.FieldConstants;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.TableConstants;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.AbstractMetaTableParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.FieldStatisticsParameters;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

public class FieldStatisticsTable extends AbstractMetaTable {

    @Override
    public void initMetaTable() {
        tableName = "field_statistics";
        schema = new TupleSchema();
        schema.addField(new VarStringInfo(TableConstants.MAX_TABLE_NAME_SIZE), "table_name");
        schema.addField(new VarStringInfo(FieldConstants.MAX_FIELD_NAME_SIZE), "field_name");
        schema.addField(new IntegerInfo(), "num_values");
        layout = new TupleLayout(schema);
    }

    @Override
    public void insertData(ChrysoTransaction txn, AbstractMetaTableParameters params) {
        FieldStatisticsParameters fsParams = (FieldStatisticsParameters) params;
        TableScan fsScan = new TableScan(txn, tableName, layout);
        fsScan.insert();
        fsScan.setData("table_name", new VarStringField(fsParams.getTableName(), TableConstants.MAX_TABLE_NAME_SIZE));
        fsScan.setData("field_name", new VarStringField(fsParams.getFieldName(), FieldConstants.MAX_FIELD_NAME_SIZE));
        fsScan.setData("num_values", new IntegerField(fsParams.getNumValues()));
        fsScan.close();
    }
    
}
