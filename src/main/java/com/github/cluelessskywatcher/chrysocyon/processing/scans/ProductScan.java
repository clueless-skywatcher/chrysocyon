package com.github.cluelessskywatcher.chrysocyon.processing.scans;

import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class ProductScan implements IScan {
    private IScan leftScan;
    private IScan rightScan;

    public ProductScan(IScan l, IScan r) {
        this.leftScan = l;
        this.rightScan = r;
    }

    @Override
    public void moveToBeginning() {
        leftScan.moveToBeginning();
        rightScan.moveToBeginning();
    }

    @Override
    public boolean next() {
        if (rightScan.next()) 
            return true;
        rightScan.moveToBeginning();
        return rightScan.next() && leftScan.next();
    }

    @Override
    public DataField getData(String fieldName) {
        if (leftScan.hasField(fieldName))
            return leftScan.getData(fieldName);
        else
            return rightScan.getData(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return leftScan.hasField(fieldName) || rightScan.hasField(fieldName);
    }

    @Override
    public void close() {
        leftScan.close();
        rightScan.close();
    }
    
}
