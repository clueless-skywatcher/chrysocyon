package com.github.cluelessskywatcher.chrysocyon.chrysql.dml;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.planning.BasicModifyPlanner;
import com.github.cluelessskywatcher.chrysocyon.planning.ModifyPlanner;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.PredicateExpression;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.QueryPredicate;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class UpdateTableStatement implements ChrySQLStatement {
    private @Getter String tableName;
    private @Getter String fieldName;
    private @Getter PredicateExpression newValue;
    private @Getter QueryPredicate predicate;
    private @Getter ChrySQLStatementResult result;

    
    public UpdateTableStatement(String tableName, String fieldName, PredicateExpression expression,
            QueryPredicate predicate) {
        this.tableName = tableName;
        this.fieldName = fieldName;
        this.newValue = expression;
        this.predicate = predicate;
        this.result = null;
    }    

    @Override
    public void execute(Chrysocyon db, ChrysoTransaction tx) {
        MetadataManager metadataManager = db.getMetadataManager();
        long timeTaken = System.currentTimeMillis();
        ModifyPlanner planner = new BasicModifyPlanner(metadataManager);
        int rowsAffected = planner.executeUpdate(this, tx);
        timeTaken = System.currentTimeMillis() - timeTaken;
        this.result = new UpdateTableResult(tableName, timeTaken, rowsAffected);
    }
}
