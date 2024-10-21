package com.github.cluelessskywatcher.chrysocyon.index;

import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleIdentifier;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;

import lombok.Getter;

public class HashedIndex implements TableIndex {
    public static int HASH_BUCKET_COUNT = 100;

    private @Getter ChrysoTransaction tx;
    private @Getter String indexName;
    private @Getter TupleLayout layout;
    private @Getter DataField searchKey = null;
    private @Getter TableScan indexScan;

    public HashedIndex(ChrysoTransaction tx, String indexName, TupleLayout layout) {
        this.tx = tx;
        this.indexName = indexName;
        this.layout = layout;
    }

    @Override
    public void moveToBeginning(DataField searchKey) {
        close();
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % HASH_BUCKET_COUNT;
        String bucketTableName = String.format("_%s_%d", indexName, bucket);
        indexScan = new TableScan(tx, bucketTableName, layout);
    }

    @Override
    public boolean next() {
        while (indexScan.next()) {
            if (indexScan.getData("dataval").equals(searchKey)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public TupleIdentifier getTupleId() {
        int blockNumber = (int) indexScan.getData("block").getValue();
        int tupleId = (int) indexScan.getData("id").getValue();
        return new TupleIdentifier(blockNumber, tupleId);
    }

    @Override
    public void insert(DataField dataVal, TupleIdentifier tupleId) {
        moveToBeginning(dataVal);
        indexScan.insert();
        indexScan.setData("block", new IntegerField(tupleId.getBlock()));
        indexScan.setData("id", new IntegerField(tupleId.getSlot()));
        indexScan.setData("dataval", dataVal);
    }

    @Override
    public void delete(DataField dataVal, TupleIdentifier tupleId) {
        moveToBeginning(dataVal);
        while (next()) {
            if (getTupleId().equals(tupleId)) {
                indexScan.delete();
                return;
            }
        }
    }

    @Override
    public void close() {
        if (indexScan != null) {
            indexScan.close();
        }
    }

    public static int searchCost(int blockCount, int recordsPerBlock) {
        return blockCount / HASH_BUCKET_COUNT;
    }
    
}
