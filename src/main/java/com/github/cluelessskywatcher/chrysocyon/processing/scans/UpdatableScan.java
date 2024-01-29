package com.github.cluelessskywatcher.chrysocyon.processing.scans;

import com.github.cluelessskywatcher.chrysocyon.tuples.TupleIdentifier;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public interface UpdatableScan extends IScan {
    public void setData(String fieldName, DataField value);
    public void insert();
    public void delete();

    public TupleIdentifier currentTupleId();
    public void moveToTuple(TupleIdentifier tid);
}
