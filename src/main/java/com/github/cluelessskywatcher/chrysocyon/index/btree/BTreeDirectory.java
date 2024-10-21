package com.github.cluelessskywatcher.chrysocyon.index.btree;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class BTreeDirectory {
    private @Getter ChrysoTransaction txn;
    private @Getter TupleLayout layout;
    private @Getter BTreePage contents;
    private @Getter String fileName;

    public BTreeDirectory(ChrysoTransaction txn, BlockIdentifier block, TupleLayout layout) {
        this.txn = txn;
        this.layout = layout;
        this.contents = new BTreePage(txn, block, layout);
        this.fileName = block.getFileName();
    }

    public void close() {
        contents.close();
    }

    public int search(DataField searchKey) {
        BlockIdentifier childBlock = findChildBlockWithKey(searchKey);
        while (contents.getFlag() > 0) {
            contents.close();
            contents = new BTreePage(txn, childBlock, layout);
            childBlock = findChildBlockWithKey(searchKey);
        }

        return childBlock.getBlockNumber();
    }

    public BTreeDirectoryEntry insert(BTreeDirectoryEntry entry) {
        if (contents.getFlag() == 0) {
            return insertEntry(entry);
        }
        BlockIdentifier childBlock = findChildBlockWithKey(entry.getDataValue());
        BTreeDirectory child = new BTreeDirectory(txn, childBlock, layout);
        BTreeDirectoryEntry newEntry = child.insert(entry);
        child.close();
        return (newEntry != null) ? insertEntry(newEntry) : null;
    }

    public void makeNewRoot(BTreeDirectoryEntry dirEntry) {
        DataField firstVal = contents.getDataValue(0);
        int level = contents.getFlag();
        BlockIdentifier newBlock = contents.split(0, level);
        BTreeDirectoryEntry oldRoot = new BTreeDirectoryEntry(firstVal, newBlock.getBlockNumber());
        insertEntry(oldRoot);
        insertEntry(dirEntry);
        contents.setFlag(level + 1);
    }

    private BTreeDirectoryEntry insertEntry(BTreeDirectoryEntry entry) {
        int newSlot = contents.findSlotBefore(entry.getDataValue()) + 1;
        contents.insertDirectory(newSlot, entry.getDataValue(), entry.getBlockNumber());
        if (!contents.isFull()) {
            return null;
        }
        int level = contents.getFlag();
        int splitPosition = contents.getTupleCount() / 2;
        DataField splitKey = contents.getDataValue(splitPosition);
        BlockIdentifier newBlock = contents.split(splitPosition, level);
        return new BTreeDirectoryEntry(splitKey, newBlock.getBlockNumber());
    }

    private BlockIdentifier findChildBlockWithKey(DataField searchKey) {
        int slot = contents.findSlotBefore(searchKey);
        if (contents.getDataValue(slot + 1).equals(searchKey)) {
            slot++;
        }
        int blockNumber = contents.getBlockNumber(slot);
        return new BlockIdentifier(fileName, blockNumber);
    }
}
