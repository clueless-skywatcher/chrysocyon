package com.github.cluelessskywatcher.chrysocyon.chrysql.dml;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;

import lombok.Getter;

public class UpdateTableResult extends ChrySQLStatementResult {
    private @Getter String tableName;
    private @Getter int rowsAffected;

    public UpdateTableResult(String tableName, long timeTaken, int rowsAffected) {
        this.tableName = tableName;
        this.timeTaken = timeTaken;
        this.rowsAffected = rowsAffected;
    }

    public String toString() {
        return String.format("UPDATE TABLE %s: %d rows affected. Time Taken: %d", tableName, rowsAffected, timeTaken);
    }
}
