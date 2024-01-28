package com.github.cluelessskywatcher.chrysocyon.chrysql.dql;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.planning.GreedyQueryPlanner;
import com.github.cluelessskywatcher.chrysocyon.planning.OpPlan;
import com.github.cluelessskywatcher.chrysocyon.planning.QueryPlanner;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.QueryPredicate;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.ProjectionScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class SelectTableStatement implements ChrySQLStatement {
    private @Getter List<String> tableNames;
    private @Getter List<String> selectFields;
    private @Getter QueryPredicate predicate;
    private @Getter ChrySQLStatementResult result;

    public SelectTableStatement(List<String> tableNames, List<String> selectFields, QueryPredicate predicate) {
        this.tableNames = tableNames;
        this.selectFields = selectFields;
        this.predicate = predicate;
    }

    public String toString() {
        StringJoiner selFields = new StringJoiner(", ");
        for (String field : selectFields) {
            selFields.add(field);
        }

        String selFieldStr = (selFields.toString().length() == 0) ? "*" : selFields.toString();

        StringJoiner tableNamesJoiner = new StringJoiner(", ");
        for (String table : tableNames) {
            tableNamesJoiner.add(table);
        }

        return String.format("select %s from %s where %s;", selFieldStr, tableNamesJoiner.toString(), predicate.toString());
    }

    @Override
    public void execute(Chrysocyon db, ChrysoTransaction txn) {
        MetadataManager mtdm = db.getMetadataManager();
        long timeTaken = System.currentTimeMillis();
        QueryPlanner planner = new GreedyQueryPlanner(mtdm);
        OpPlan plan = planner.createPlan(this, txn);
        ProjectionScan s = (ProjectionScan) plan.open();
        List<String> fields = s.getFields();
        List<List<DataField>> rows = new ArrayList<>();
        while (s.next()) {
            List<DataField> row = new ArrayList<>();
            for (String field : fields) {
                row.add(s.getData(field));
            }
            rows.add(row);
        }
        timeTaken = System.currentTimeMillis() - timeTaken;
        this.result = new SelectTableResult(fields, rows, timeTaken);
    }
}
