package com.github.cluelessskywatcher.chrysocyon.processing.scans;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.transactions.concurrency.exceptions.LockAbortedException;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleIdentifier;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TuplePage;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class TableScan implements UpdatableScan {
    private ChrysoTransaction tx;
    private String fileName;
    private TupleLayout layout;
    private TuplePage page;
    private @Getter int currentSlot;

    public TableScan(ChrysoTransaction tx, String tableName, TupleLayout layout) {
        this.tx = tx;
        this.layout = layout;
        this.fileName = tableName + ".dat";
        try {
            if (this.tx.getSize(fileName) == 0) {
                moveToNewBlock();
            }
            else {
                moveToBlock(0);
            }
        }
        catch (LockAbortedException e) {
            System.out.println(tableName);
        }
    }

    public void close() {
        if (page != null) {
            tx.unpin(page.getBlock());
        }
    }

    public boolean hasField(String fieldName) {
        return layout.getSchema().hasField(fieldName);
    }

    public void moveToBeginning() {
        moveToBlock(0);
    }

    public boolean next() {
        currentSlot = page.nextSlot(currentSlot);
        while (currentSlot < 0) {
            if (lastBlock()) {
                return false;
            }
            moveToBlock(page.getBlock().getBlockNumber() + 1);
            currentSlot = page.nextSlot(currentSlot);
        }
        return true;
    }

    public void moveToTuple(TupleIdentifier tid) {
        close();
        BlockIdentifier block = new BlockIdentifier(fileName, tid.getBlock());
        page = new TuplePage(tx, block, layout);
        currentSlot = tid.getSlot();
    }

    public void insert() {
        currentSlot = page.nextSlotToInsert(currentSlot);
        while (currentSlot < 0) {
            if (lastBlock()) {
                moveToNewBlock();
            }
            else {
                moveToBlock(page.getBlock().getBlockNumber() + 1);
            }
            currentSlot = page.nextSlotToInsert(currentSlot);
        }
    }

    public DataField getData(String fieldName) {
        return page.getData(currentSlot, fieldName);
    }

    public void setData(String fieldName, DataField value) {
        page.setData(currentSlot, fieldName, value);
    }
    
    public TupleIdentifier currentTupleId() {
        return new TupleIdentifier(page.getBlock().getBlockNumber(), currentSlot);
    }

    public void delete() {
        page.delete(currentSlot);
    }

    private void moveToBlock(int block) {
        close();
        BlockIdentifier blk = new BlockIdentifier(fileName, block);
        page = new TuplePage(tx, blk, layout);
        currentSlot = -1;
    }

    private void moveToNewBlock() {
        close();
        BlockIdentifier blk = tx.append(fileName);
        page = new TuplePage(tx, blk, layout);
        page.setDefaults();
        currentSlot = -1;
    }

    private boolean lastBlock() {
        return page.getBlock().getBlockNumber() == tx.getSize(fileName) - 1;
    }
}
