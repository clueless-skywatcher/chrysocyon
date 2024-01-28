package com.github.cluelessskywatcher.chrysocyon.processing.scans;

import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public interface IScan {
    public void moveToBeginning();
    public boolean next();
    public DataField getData(String fieldName);
    public boolean hasField(String fieldName);
    public void close();
}
