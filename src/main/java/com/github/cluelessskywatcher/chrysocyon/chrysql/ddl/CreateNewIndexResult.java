package com.github.cluelessskywatcher.chrysocyon.chrysql.ddl;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;

import lombok.Getter;

public class CreateNewIndexResult extends ChrySQLStatementResult {
    
    private @Getter String indexName;
    private @Getter String tableName;
    private @Getter String fieldName;

    public CreateNewIndexResult(String indexName, String tableName, String fieldName, long timeTaken) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.timeTaken = timeTaken;
    }

    public String toString() {
        return String.format("CREATE INDEX. Time taken: %s", timeTaken);
    }
}
