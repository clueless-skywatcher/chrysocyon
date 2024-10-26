package com.github.cluelessskywatcher.chrysocyon.materialization;

import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.UpdatableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

import lombok.Getter;

public class TemporaryTable {
    private static int nextTableNumber = 0;
    private @Getter ChrysoTransaction txn;
    private @Getter String name;
    private @Getter TupleLayout layout;

    public TemporaryTable(ChrysoTransaction txn, TupleSchema schema) {
        this.txn = txn;
        this.name = nextTableName();
        this.layout = new TupleLayout(schema);
    }

    public UpdatableScan open() {
        return new TableScan(txn, name, layout);
    }

    private String nextTableName() {
        nextTableNumber++;
        return String.format("temp_%d", nextTableNumber);
    }
}
