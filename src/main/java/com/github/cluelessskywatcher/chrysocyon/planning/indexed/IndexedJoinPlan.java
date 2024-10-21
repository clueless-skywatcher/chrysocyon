package com.github.cluelessskywatcher.chrysocyon.planning.indexed;

import com.github.cluelessskywatcher.chrysocyon.index.TableIndex;
import com.github.cluelessskywatcher.chrysocyon.metadata.IndexInfo;
import com.github.cluelessskywatcher.chrysocyon.planning.DatabasePlan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.indexed.IndexedJoinScan;;

public class IndexedJoinPlan implements DatabasePlan {
    private DatabasePlan p1, p2;
    private IndexInfo indexInfo;
    private String lhsJoinField;
    private TupleSchema schema = new TupleSchema();

    public IndexedJoinPlan(DatabasePlan p1, DatabasePlan p2, IndexInfo indexInfo, String lhsJoinField) {
        this.p1 = p1;
        this.p2 = p2;
        this.indexInfo = indexInfo;
        this.lhsJoinField = lhsJoinField;
    }

    @Override
    public IScan open() {
        IScan s1 = p1.open();
        TableScan s2 = (TableScan) p2.open();
        TableIndex index = indexInfo.open();
        return new IndexedJoinScan(s1, index, lhsJoinField, s2);
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed() + (p1.recordsOutput() * indexInfo.getBlocksAccessed())
                                + recordsOutput();
    }

    @Override
    public int recordsOutput() {
        return p1.recordsOutput() * indexInfo.getBlocksAccessed();
    }

    @Override
    public int distinctValues(String fieldName) {
        return p1.schema().hasField(fieldName) ? p1.distinctValues(fieldName) : p2.distinctValues(fieldName);
    }

    @Override
    public TupleSchema schema() {
        return schema;
    }
}
