package com.github.cluelessskywatcher.chrysocyon.tuples.info;

import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

import lombok.Getter;

public class VarStringInfo implements DataInfo {
    private @Getter int charSize;
    
    public VarStringInfo(int charSize) {
        this.charSize = charSize;
    }

    public int getSize() {
        return PageObject.maxStringLength(charSize);
    }

    @Override
    public TupleDataType getDataType() {
        return TupleDataType.VARSTR;
    }

    public String toString() {
        return String.format("varstr(%d)", charSize);
    }
}
