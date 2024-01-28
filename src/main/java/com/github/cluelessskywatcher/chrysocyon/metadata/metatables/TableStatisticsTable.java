package com.github.cluelessskywatcher.chrysocyon.metadata.metatables;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.TableConstants;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.ViewConstants;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.AbstractMetaTableParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.TableStatisticsParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.ViewCatalogParameters;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

public class TableStatisticsTable extends AbstractMetaTable {

    @Override
    public void initMetaTable() {
        tableName = "table_statistics";
        schema = new TupleSchema();
        schema.addField(new VarStringInfo(TableConstants.MAX_TABLE_NAME_SIZE), "table_name");
        schema.addField(new IntegerInfo(), "num_blocks");
        schema.addField(new IntegerInfo(), "num_records");
        layout = new TupleLayout(schema);
    }

    @Override
    public void insertData(ChrysoTransaction txn, AbstractMetaTableParameters params) {
        TableStatisticsParameters tsParams = (TableStatisticsParameters) params;
        
        TableScan tsScan = new TableScan(txn, tableName, layout);
        tsScan.setData("table_name", new VarStringField(tsParams.getTableName(), TableConstants.MAX_TABLE_NAME_SIZE));
        tsScan.setData("num_blocks", new IntegerField(tsParams.getNumBlocks()));
        tsScan.setData("num_records", new IntegerField(tsParams.getNumRecords()));
        tsScan.close();
    }
    
}
