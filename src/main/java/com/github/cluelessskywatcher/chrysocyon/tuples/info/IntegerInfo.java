package com.github.cluelessskywatcher.chrysocyon.tuples.info;

public class IntegerInfo implements DataInfo {

    @Override
    public int getSize() {
        return Integer.BYTES;
    }

    @Override
    public TupleDataType getDataType() {
        return TupleDataType.INTEGER;
    }
    
}
