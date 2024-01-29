package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

public interface OpPlan {
    public IScan open();
    public int blocksAccessed();
    public int recordsOutput();
    public int distinctValues(String fieldName);

    public TupleSchema schema();
}
