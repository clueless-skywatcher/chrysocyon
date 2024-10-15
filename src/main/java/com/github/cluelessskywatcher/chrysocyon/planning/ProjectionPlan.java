package com.github.cluelessskywatcher.chrysocyon.planning;

import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.ProjectionScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

public class ProjectionPlan implements DatabasePlan {
    private DatabasePlan plan;
    private TupleSchema schema;

    public ProjectionPlan(DatabasePlan plan, List<String> fields) {
        this.plan = plan;
        TupleSchema newSchema = new TupleSchema();
        if (fields.size() == 0) {
            newSchema.addAll(plan.schema());
        }
        else {
            for (String field : fields) {
                newSchema.addFromSchema(field, plan.schema());
            }
        }
        this.schema = newSchema;
    }

    @Override
    public IScan open() {
        return new ProjectionScan(plan.open(), schema.getFields());
    }

    @Override
    public int blocksAccessed() {
        return plan.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return plan.distinctValues(fieldName);
    }

    @Override
    public TupleSchema schema() {
        return schema;
    }
}
