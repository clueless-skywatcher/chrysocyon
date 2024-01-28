package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.metadata.StatisticalInfo;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

public class TablePlan implements OpPlan {
    private ChrysoTransaction txn;
    private String tableName;
    private TupleLayout layout;
    private StatisticalInfo statInfo;

    public TablePlan(String tableName, MetadataManager mtdm, ChrysoTransaction txn) {
        this.tableName = tableName;
        this.txn = txn;
        this.layout = mtdm.getLayout(tableName, txn);
        this.statInfo = mtdm.getStatsInfo(tableName, layout, txn);
    }

    @Override
    public IScan open() {
        return new TableScan(txn, tableName, layout);
    }

    @Override
    public int blocksAccessed() {
        return statInfo.getNumBlocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return statInfo.getNumRecords();
    }

    @Override
    public int distinctValues(String fieldName) {
        return statInfo.getDistinctValues(fieldName);
    }

    @Override
    public TupleSchema schema() {
        return layout.getSchema();
    }
}
