package com.github.cluelessskywatcher.chrysocyon.processing.scans;

import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class RenamingScan implements IScan {
    private IScan scan;
    private String fieldName;
    private String renameTo;

    public RenamingScan(IScan scan, String fieldName, String renameTo) {
        this.scan = scan;
        if (!scan.hasField(fieldName)) {
            throw new RuntimeException(String.format("Field %s does not exist", fieldName));
        }
        this.fieldName = fieldName;
        this.renameTo = renameTo;
    }

    @Override
    public void moveToBeginning() {
        scan.moveToBeginning();
    }

    @Override
    public boolean next() {
        return scan.next();
    }

    @Override
    public DataField getData(String fieldName) {
        if (fieldName.equals(renameTo)) {
            return scan.getData(this.fieldName);
        }
        return scan.getData(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        if (fieldName.equals(renameTo)) return true;
        return scan.hasField(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }
    
}
