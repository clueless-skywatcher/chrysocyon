package com.github.cluelessskywatcher.chrysocyon.chrysql.dml;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.planning.BasicModifyPlanner;
import com.github.cluelessskywatcher.chrysocyon.planning.ModifyPlanner;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.QueryPredicate;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DeleteFromTableStatement implements ChrySQLStatement {
    private @Getter String tableName;
    private @Getter QueryPredicate predicate;
    private @Getter ChrySQLStatementResult result;

    public DeleteFromTableStatement(String tableName, QueryPredicate predicate) {
        this.tableName = tableName;
        this.predicate = predicate;
        this.result = null;
    }

    @Override
    public void execute(Chrysocyon db, ChrysoTransaction tx) {
        MetadataManager metadataManager = db.getMetadataManager();
        long timeTaken = System.currentTimeMillis();
        ModifyPlanner planner = new BasicModifyPlanner(metadataManager);
        int rowsAffected = planner.executeDelete(this, tx);
        timeTaken = System.currentTimeMillis() - timeTaken;
        this.result = new DeleteFromTableResult(tableName, timeTaken, rowsAffected);
    }

    public String toString() {
        String predString = "";
        if (predicate != null) {
            predString = predicate.toString();
        }
        return String.format("DELETE FROM %s %s;", tableName, predString);
    }
}
