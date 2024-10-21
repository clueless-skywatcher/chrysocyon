package com.github.cluelessskywatcher.chrysocyon.index.btree;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.index.TableIndex;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleIdentifier;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.DataInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

import lombok.Getter;

public class BTreeIndex implements TableIndex {
    private @Getter ChrysoTransaction txn;
    private @Getter TupleLayout leafLayout, directoryLayout;
    private @Getter String leafTable;
    private BTreeLeaf leaf = null;
    private @Getter BlockIdentifier root;

    public BTreeIndex(ChrysoTransaction txn, String indexName, TupleLayout leafLayout) {
        this.txn = txn;
        
        this.leafTable = String.format("_%s_leaf", indexName);
        this.leafLayout = leafLayout;

        if (txn.getSize(leafTable) == 0) {
            BlockIdentifier block = txn.append(leafTable);
            BTreePage node = new BTreePage(txn, block, leafLayout);
            node.format(block, -1);
        }

        TupleSchema directorySchema = new TupleSchema();
        directorySchema.addFromSchema("block", leafLayout.getSchema());
        directorySchema.addFromSchema("dataval", leafLayout.getSchema());
        String directoryTable = String.format("%s_dir", indexName);
        this.directoryLayout = new TupleLayout(directorySchema);

        root = new BlockIdentifier(directoryTable, 0);

        if (txn.getSize(directoryTable) == 0) {
            txn.append(directoryTable);
            BTreePage node = new BTreePage(txn, root, directoryLayout);
            node.format(root, 0);

            DataInfo fieldType = directorySchema.getField("dataval");
            
            DataField minimumValue;

            if (fieldType instanceof IntegerInfo) {
                minimumValue = new IntegerField(Integer.MIN_VALUE);
            } else {
                minimumValue = new VarStringField("", ((VarStringInfo) fieldType).getCharSize());
            }

            node.insertDirectory(0, minimumValue, 0);
            node.close();
        }
    }

    @Override
    public void moveToBeginning(DataField searchKey) {
        close();
        BTreeDirectory rootDir = new BTreeDirectory(txn, root, directoryLayout);
        int blockNumber = rootDir.search(searchKey);
        rootDir.close();
        BlockIdentifier leafBlock = new BlockIdentifier(leafTable, blockNumber);
        leaf = new BTreeLeaf(txn, leafBlock, directoryLayout, searchKey);
    }

    @Override
    public boolean next() {
        return leaf.next();
    }

    @Override
    public TupleIdentifier getTupleId() {
        return leaf.getTupleId();
    }

    @Override
    public void insert(DataField dataVal, TupleIdentifier tupleId) {
        moveToBeginning(dataVal);
        BTreeDirectoryEntry entry = leaf.insert(tupleId);
        leaf.close();

        if (entry == null) return;

        BTreeDirectory rootDir = new BTreeDirectory(txn, root, directoryLayout);
        BTreeDirectoryEntry entry2 = rootDir.insert(entry);

        if (entry2 != null) {
            rootDir.makeNewRoot(entry2);
        }
        rootDir.close();
    }

    @Override
    public void delete(DataField dataVal, TupleIdentifier tupleId) {
        moveToBeginning(dataVal);
        leaf.delete(tupleId);
        leaf.close();
    }

    @Override
    public void close() {
        if (leaf != null) leaf.close();
    }

    public static int searchCost(int blockCount, int recordsPerBlock) {
        return 1 + (int) (Math.log(blockCount) / Math.log(recordsPerBlock));
    }
    
}
