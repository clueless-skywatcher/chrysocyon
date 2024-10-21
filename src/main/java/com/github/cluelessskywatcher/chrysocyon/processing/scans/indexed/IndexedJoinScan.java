package com.github.cluelessskywatcher.chrysocyon.processing.scans.indexed;

import com.github.cluelessskywatcher.chrysocyon.index.TableIndex;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class IndexedJoinScan implements IScan {
    private IScan lhs;
    private TableIndex index;
    private String lhsJoinField;
    private TableScan rhs;

    public IndexedJoinScan(IScan s1, TableIndex index, String lhsJoinField, TableScan s2) {
        this.lhs = s1;
        this.index = index;
        this.lhsJoinField = lhsJoinField;
        this.rhs = s2;
        moveToBeginning();
    }

    @Override
    public void moveToBeginning() {
        lhs.moveToBeginning();
        lhs.next();
        resetIndex();
    }

    private void resetIndex() {
        DataField searchKey = lhs.getData(lhsJoinField);
        index.moveToBeginning(searchKey);
    }

    @Override
    public boolean next() {
        while (true) {
            if (index.next()) {
                rhs.moveToTuple(index.getTupleId());
                return true;
            }
            if (!lhs.next()) {
                return false;
            }
            resetIndex();
        }
    }

    @Override
    public DataField getData(String fieldName) {
        if (rhs.hasField(fieldName)) {
            return rhs.getData(fieldName);
        } else {
            return lhs.getData(fieldName);
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return rhs.hasField(fieldName) || lhs.hasField(fieldName);
    }

    @Override
    public void close() {
        lhs.close();
        index.close();
        rhs.close();
    }    
}
