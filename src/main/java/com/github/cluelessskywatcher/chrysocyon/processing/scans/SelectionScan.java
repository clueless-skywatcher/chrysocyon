package com.github.cluelessskywatcher.chrysocyon.processing.scans;

import com.github.cluelessskywatcher.chrysocyon.processing.expressions.QueryPredicate;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleIdentifier;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class SelectionScan implements UpdatableScan {
    private IScan scan; 
    private QueryPredicate predicate;

    public SelectionScan(IScan scan, QueryPredicate predicate) {
        this.scan = scan;
        this.predicate = predicate;
    }

    @Override
    public void moveToBeginning() {
        scan.moveToBeginning();
    }

    @Override
    public boolean next() {
        while (scan.next()) {
            if (predicate.isSatisfied(scan)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public DataField getData(String fieldName) {
        return scan.getData(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan.hasField(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }

    @Override
    public void setData(String fieldName, DataField value) {
        UpdatableScan uScan = (UpdatableScan) scan;
        uScan.setData(fieldName, value);
    }

    @Override
    public void insert() {
        UpdatableScan uScan = (UpdatableScan) scan;
        uScan.insert();
    }

    @Override
    public void delete() {
        UpdatableScan uScan = (UpdatableScan) scan;
        uScan.delete();
    }

    @Override
    public TupleIdentifier currentTupleId() {
        UpdatableScan uScan = (UpdatableScan) scan;
        return uScan.currentTupleId();
    }

    @Override
    public void moveToTuple(TupleIdentifier tid) {
        UpdatableScan uScan = (UpdatableScan) scan;
        uScan.moveToTuple(tid);
    }
    
}
