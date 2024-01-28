package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.InsertTableStatement;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.UpdatableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import java.util.Iterator;

public class BasicModifyPlanner implements ModifyPlanner {
    private MetadataManager mtdm;

    public BasicModifyPlanner(MetadataManager mtdm) {
        this.mtdm = mtdm;
    }

    @Override
    public int executeCreateTable(CreateTableStatement stmt, ChrysoTransaction txn) {
        mtdm.createTable(stmt.getTableName(), stmt.getSchema(), txn);
        return 0;
    }

    @Override
    public int executeInsert(InsertTableStatement stmt, ChrysoTransaction txn) {
        // TODO Auto-generated method stub
        OpPlan plan = new TablePlan(stmt.getTableName(), mtdm, txn);
        UpdatableScan scan = (UpdatableScan) plan.open();
        scan.insert();
        Iterator<DataField> iterator = stmt.getValues().iterator();

        for (String field : stmt.getFieldNames()) {
            DataField val = iterator.next();
            scan.setData(field, val);
        }

        scan.close();
        return 1;
    }

    
}
