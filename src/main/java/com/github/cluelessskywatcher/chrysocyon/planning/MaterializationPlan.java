package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.materialization.TemporaryTable;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.UpdatableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

public class MaterializationPlan implements DatabasePlan {
    private DatabasePlan underlyingPlan;
    private ChrysoTransaction txn;

    public MaterializationPlan(ChrysoTransaction txn, DatabasePlan plan) {
        this.underlyingPlan = plan;
        this.txn = txn;
    }

    @Override
    public IScan open() {
        TupleSchema schema = underlyingPlan.schema();
        TemporaryTable tempTable = new TemporaryTable(txn, schema);
        IScan sourceScan = underlyingPlan.open();
        UpdatableScan destinationScan = tempTable.open();
        while (sourceScan.next()) {
            destinationScan.next();
            for (String fieldName: schema.getFields()) {
                destinationScan.setData(fieldName, sourceScan.getData(fieldName));
            }
        }

        sourceScan.close();
        destinationScan.moveToBeginning();
        return destinationScan;
    }

    @Override
    public int blocksAccessed() {
        TupleLayout layout = new TupleLayout(underlyingPlan.schema());
        double recordsPerBlock = (double)(txn.getBlockSize() / layout.getSlotSize());
        return (int)(Math.ceil(underlyingPlan.recordsOutput() / recordsPerBlock));
    }

    @Override
    public int recordsOutput() {
        return underlyingPlan.recordsOutput();
    }
    @Override
    public int distinctValues(String fieldName) {
        return underlyingPlan.distinctValues(fieldName);
    }

    @Override
    public TupleSchema schema() {
        return this.underlyingPlan.schema();
    }


}
