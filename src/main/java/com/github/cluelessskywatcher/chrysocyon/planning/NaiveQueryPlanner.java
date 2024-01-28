package com.github.cluelessskywatcher.chrysocyon.planning;

import java.util.ArrayList;
import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLParser;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dql.SelectTableStatement;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public class NaiveQueryPlanner implements QueryPlanner {
    private MetadataManager mtdm;

    public NaiveQueryPlanner(MetadataManager mtdm) {
        this.mtdm = mtdm;
    }

    @Override
    public OpPlan createPlan(SelectTableStatement stmt, ChrysoTransaction txn) {
        List<OpPlan> plans = new ArrayList<>();
        for (String tableName : stmt.getTableNames()) {
            String viewDef = mtdm.getViewDefinition(tableName, txn);
            if (viewDef != null) {
                ChrySQLParser parser = new ChrySQLParser(viewDef);
                SelectTableStatement viewStmt = (SelectTableStatement) parser.parseSelect();
                plans.add(createPlan(viewStmt, txn));
            }
            else {
                plans.add(new TablePlan(tableName, mtdm, txn));
            }
        }

        OpPlan plan = plans.remove(0);
        for (OpPlan otherPlan : plans) {
            plan = new ProductPlan(plan, otherPlan);
        }

        plan = new SelectionPlan(plan, stmt.getPredicate());

        return new ProjectionPlan(plan, stmt.getSelectFields());
    }
    
}
