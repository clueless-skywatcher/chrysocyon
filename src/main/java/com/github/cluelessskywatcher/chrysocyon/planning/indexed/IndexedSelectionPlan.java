package com.github.cluelessskywatcher.chrysocyon.planning.indexed;

import com.github.cluelessskywatcher.chrysocyon.index.TableIndex;
import com.github.cluelessskywatcher.chrysocyon.metadata.IndexInfo;
import com.github.cluelessskywatcher.chrysocyon.planning.DatabasePlan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.indexed.IndexedSelectionScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class IndexedSelectionPlan implements DatabasePlan {
    private DatabasePlan p;
    private IndexInfo indexInfo;
    private DataField val;

    public IndexedSelectionPlan(DatabasePlan p, IndexInfo indexInfo, DataField val) {
        this.p = p;
        this.indexInfo = indexInfo;
        this.val = val;
    }

    @Override
    public IScan open() {
        TableScan scan = (TableScan) p.open();
        TableIndex index = indexInfo.open();
        return new IndexedSelectionScan(index, val, scan);
    }

    @Override
    public int blocksAccessed() {
        return indexInfo.getBlocksAccessed() + recordsOutput();
    }

    @Override
    public int recordsOutput() {
        return indexInfo.getRecordCount();
    }

    @Override
    public int distinctValues(String fieldName) {
        return indexInfo.getDistinctValues(fieldName);
    }

    @Override
    public TupleSchema schema() {
        return indexInfo.getSchema();
    }
    
}
