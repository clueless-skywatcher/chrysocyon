package com.github.cluelessskywatcher.chrysocyon.processing.scans;

import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.metadata.exceptions.FieldDoesNotExistException;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class ProjectionScan implements IScan {
    private IScan scan;
    private @Getter List<String> fields;

    public ProjectionScan(IScan scan, List<String> fields) {
        this.scan = scan;
        this.fields = fields;
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
        if (hasField(fieldName)) {
            return scan.getData(fieldName);
        }
        else {
            throw new FieldDoesNotExistException(String.format("Field %s not found", fieldName));
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }
    
}
