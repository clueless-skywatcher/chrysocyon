package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.processing.expressions.QueryPredicate;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.SelectionScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

public class SelectionPlan implements OpPlan {
    private OpPlan plan;
    private QueryPredicate pred;

    public SelectionPlan(OpPlan plan, QueryPredicate p) {
        this.plan = plan;
        this.pred = p;
    }

    @Override
    public IScan open() {
        IScan scan = plan.open();
        return new SelectionScan(scan, pred);
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput() / pred.reductionFactor(plan);
    }

    @Override
    public int distinctValues(String fieldName) {
        if (pred.equatesWithConstant(fieldName) != null) {
            return 1;
        }
        else {
            String fieldName2 = pred.equatesWithField(fieldName);
            if (fieldName2 != null) {
                return Math.min(plan.distinctValues(fieldName), plan.distinctValues(fieldName2));
            }
            else {
                return plan.distinctValues(fieldName);
            }
        }
    }

    @Override
    public TupleSchema schema() {
        return plan.schema();
    }
    
}
