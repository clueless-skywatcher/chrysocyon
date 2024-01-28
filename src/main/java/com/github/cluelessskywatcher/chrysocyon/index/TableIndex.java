package com.github.cluelessskywatcher.chrysocyon.index;

import com.github.cluelessskywatcher.chrysocyon.tuples.TupleIdentifier;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public interface TableIndex {
    public void moveToBeginning(DataField searchKey);
    public boolean next();
    public TupleIdentifier getTupleId();
    
    public void insert(DataField dataVal, TupleIdentifier tupleId);
    public void delete(DataField dataVal, TupleIdentifier tupleId);

    public void close();
}
