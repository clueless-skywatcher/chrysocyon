package com.github.cluelessskywatcher.chrysocyon.chrysql.ddl;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.Getter;

public class CreateNewIndexStatement implements ChrySQLStatement {
    private @Getter String indexName;
    private @Getter String tableName;
    private @Getter String fieldName;
    private @Getter ChrySQLStatementResult result;

    public CreateNewIndexStatement(String indexName, String tableName, String fieldName) {
        this.fieldName = fieldName;
        this.tableName = tableName;
        this.indexName = indexName;
    }
    
    @Override
    public void execute(Chrysocyon db, ChrysoTransaction tx) {
        long timeTaken = System.currentTimeMillis();
        // TODO: Main logic
        timeTaken = System.currentTimeMillis() - timeTaken;
        this.result = new CreateNewIndexResult(indexName, tableName, fieldName, timeTaken);
    }

    public String toString() {
        return String.format("create index %s on %s (%s)", indexName, tableName, fieldName);
    }
}
