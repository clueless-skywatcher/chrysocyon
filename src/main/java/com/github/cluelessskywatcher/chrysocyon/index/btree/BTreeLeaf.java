package com.github.cluelessskywatcher.chrysocyon.index.btree;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleIdentifier;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class BTreeLeaf {
    private @Getter ChrysoTransaction txn;
    private @Getter TupleLayout layout;
    private @Getter DataField searchKey;
    private @Getter BTreePage contents;
    private @Getter int currentSlot;
    private @Getter String fileName;

    public BTreeLeaf(ChrysoTransaction txn, BlockIdentifier blockId, TupleLayout layout,
                        DataField key) {
        this.txn = txn;
        this.layout = layout;
        this.searchKey = key;
        contents = new BTreePage(txn, blockId, layout);
        currentSlot = contents.findSlotBefore(key);
        this.fileName = blockId.getFileName();
    }

    public void close() {
        contents.close();
    }

    public boolean next() {
        currentSlot++;
        if (currentSlot >= contents.getTupleCount()) {
            return tryOverflow();
        } else if (contents.getDataValue(currentSlot).equals(searchKey)) {
            return true;
        } else {
            return tryOverflow();
        }
    }

    public TupleIdentifier getTupleId() {
        return contents.getTupleID(currentSlot);
    }

    public void delete(TupleIdentifier tupleId) {
        while (next()) {
            if (getTupleId().equals(tupleId)) {
                contents.delete(currentSlot);
                return;
            }
        }
    }

    public BTreeDirectoryEntry insert(TupleIdentifier tupleId) {
        // If the current page is an overflow block we need to handle this separately. Check whether
        // this block is an overflow block (flag will be >= 0) and whether the search key is 
        // less than the value in the overflow block
        if (contents.getFlag() >= 0 && contents.getDataValue(0).compareTo(searchKey) > 0) {
            // Get the first value in this block
            DataField firstVal = contents.getDataValue(0);
            // Split at the first position, creating a new overflow block
            BlockIdentifier newBlock = contents.split(0, contents.getFlag());
            // Move to the first position of the block
            currentSlot = 0;
            // Set this block to no longer being an overflow block
            contents.setFlag(-1);
            // Insert the searchKey in this position
            contents.insertLeaf(currentSlot, searchKey, tupleId);
            // Return the new overflow block
            return new BTreeDirectoryEntry(firstVal, newBlock.getBlockNumber());
        }
        currentSlot++;
        contents.insertLeaf(currentSlot, searchKey, tupleId);
        if (!contents.isFull()) {
            return null;
        }
        DataField firstKey = contents.getDataValue(0);
        DataField lastKey = contents.getDataValue(contents.getTupleCount() - 1);
        if (lastKey.equals(firstKey)) {
            BlockIdentifier overflowBlock = contents.split(1, contents.getFlag());
            contents.setFlag(overflowBlock.getBlockNumber());
            return null;
        } else {
            int splitPosition = contents.getTupleCount() / 2;
            DataField splitKey = contents.getDataValue(splitPosition);
            if (splitKey.equals(firstKey)) {
                while (contents.getDataValue(splitPosition).equals(splitKey)) {
                    splitPosition++;
                }
                splitKey = contents.getDataValue(splitPosition);
            } else {
                while (contents.getDataValue(splitPosition - 1).equals(splitKey)) {
                    splitPosition--;
                }
            }
            BlockIdentifier newBlock = contents.split(splitPosition - 1, -1);
            return new BTreeDirectoryEntry(splitKey, newBlock.getBlockNumber());
        }
    }

    private boolean tryOverflow() {
        DataField firstKey = contents.getDataValue(0);
        int flag = contents.getFlag();
        if (!searchKey.equals(firstKey) || flag < 0) {
            return false;
        }
        contents.close();
        BlockIdentifier newBlock = new BlockIdentifier(fileName, flag);
        contents = new BTreePage(txn, newBlock, layout);
        currentSlot = 0;
        return true;
    }
}
