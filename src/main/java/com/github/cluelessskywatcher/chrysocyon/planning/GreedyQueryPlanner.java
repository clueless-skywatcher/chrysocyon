package com.github.cluelessskywatcher.chrysocyon.planning;

import java.util.ArrayList;
import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLParser;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dql.SelectTableStatement;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

/**
 * Generate a greedy query planner that minimizes the cost
 * at each product step
 */
public class GreedyQueryPlanner implements QueryPlanner {
    private MetadataManager mtdm;

    public GreedyQueryPlanner(MetadataManager mtdm) {
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
            OpPlan plan1 = new ProductPlan(plan, otherPlan);
            OpPlan plan2 = new ProductPlan(otherPlan, plan);
            plan = (plan1.blocksAccessed() < plan2.blocksAccessed()) ? plan1 : plan2;
        }

        plan = new SelectionPlan(plan, stmt.getPredicate());

        return new ProjectionPlan(plan, stmt.getSelectFields());
    }
    
}
