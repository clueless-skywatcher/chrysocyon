package com.github.cluelessskywatcher.chrysocyon.processing.scans.indexed;

import com.github.cluelessskywatcher.chrysocyon.index.TableIndex;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleIdentifier;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class IndexedSelectionScan implements IScan {
    private TableScan scan;
    private DataField val;
    private TableIndex index;

    public IndexedSelectionScan(TableIndex index, DataField val, TableScan scan) {
        this.index = index;
        this.val = val;
        this.scan = scan;
        moveToBeginning();
    }

    @Override
    public void moveToBeginning() {
        index.moveToBeginning(val);
    }

    @Override
    public boolean next() {
        boolean ok = index.next();
        if (ok) {
            TupleIdentifier tuple = index.getTupleId();
            scan.moveToTuple(tuple);
        }
        return ok;
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
        index.close();
        scan.close();
    }
    
}
