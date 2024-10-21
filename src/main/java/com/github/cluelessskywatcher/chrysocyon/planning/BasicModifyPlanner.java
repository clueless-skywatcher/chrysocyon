package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateNewIndexStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateNewTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateNewViewStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.InsertIntoTableStatement;
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
    public int executeCreateTable(CreateNewTableStatement stmt, ChrysoTransaction txn) {
        mtdm.createTable(stmt.getTableName(), stmt.getSchema(), txn);
        return 0;
    }

    @Override
    public int executeInsert(InsertIntoTableStatement stmt, ChrysoTransaction txn) {
        DatabasePlan plan = new TablePlan(stmt.getTableName(), mtdm, txn);
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

    @Override
    public int executeCreateIndex(CreateNewIndexStatement stmt, ChrysoTransaction txn) {
        mtdm.createIndex(stmt.getIndexName(), stmt.getTableName(), stmt.getFieldName(), txn);
        return 1;
    }

    @Override
    public int executeCreateView(CreateNewViewStatement stmt, ChrysoTransaction txn) {
        mtdm.createView(stmt.getViewName(), stmt.getQuery().toString(), txn);
        return 1;
    }
}
