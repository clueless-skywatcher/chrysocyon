package com.github.cluelessskywatcher.chrysocyon.chrysql.dml;

import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class InsertIntoTableResult extends ChrySQLStatementResult {
    private @Getter String tableName;
    private @Getter List<List<DataField>> rows;
    
    public InsertIntoTableResult(String tableName, long timeTaken) {
        this.tableName = tableName;
        this.timeTaken = timeTaken;
    }

    public String toString() {
        return String.format("INSERT INTO %s. Time taken: %d ms", tableName, timeTaken);
    }
}
