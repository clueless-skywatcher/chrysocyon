package com.github.cluelessskywatcher.chrysocyon.chrysql.dml;

import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class DeleteFromTableResult extends ChrySQLStatementResult {
    private @Getter String tableName;
    private @Getter List<List<DataField>> rows;
    private @Getter List<String> fields;
    private @Getter int rowsAffected;
    
    public DeleteFromTableResult(String tableName, long timeTaken, int rowsAffected) {
        this.tableName = tableName;
        this.rowsAffected = rowsAffected;
        this.timeTaken = timeTaken;
    }

    public String toString() {
        return String.format("DELETE FROM %s: %d rows affected. Time Taken: %d", tableName, rowsAffected, timeTaken);
    }
}
