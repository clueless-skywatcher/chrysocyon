package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.ProductScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

public class ProductPlan implements DatabasePlan {
    private DatabasePlan planL, planR;
    private TupleSchema schema;

    public ProductPlan(DatabasePlan planL, DatabasePlan planR) {
        this.planL = planL;
        this.planR = planR;
        this.schema = new TupleSchema();
        this.schema.addAll(planL.schema());
        this.schema.addAll(planR.schema());
    }

    @Override
    public IScan open() {
        return new ProductScan(planL.open(), planR.open());
    }

    @Override
    public int blocksAccessed() {
        return planL.blocksAccessed() + planL.recordsOutput() * planR.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return planL.recordsOutput() * planR.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        if (planL.schema().hasField(fieldName))
            return planL.distinctValues(fieldName);
        else
            return planR.distinctValues(fieldName);
    }

    @Override
    public TupleSchema schema() {
        return schema;
    }
}
