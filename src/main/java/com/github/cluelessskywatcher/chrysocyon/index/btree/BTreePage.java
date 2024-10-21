package com.github.cluelessskywatcher.chrysocyon.index.btree;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleIdentifier;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.TupleDataType;

import lombok.Getter;

/**
 * A B-Tree page will have the following format
 * Byte 0-3: Flag of the page, denoting the level of the page
 * Byte 4-7: Number of tuples present
 * Byte 8-...: Records
 */
public class BTreePage {
    private ChrysoTransaction tx;
    private @Getter BlockIdentifier currentBlock;
    private @Getter TupleLayout layout;

    public BTreePage(ChrysoTransaction tx, BlockIdentifier blockId, TupleLayout layout) {
        this.tx = tx;
        this.currentBlock = blockId;
        this.layout = layout;
        tx.pin(currentBlock);
    }

    public int findSlotBefore(DataField searchKey) {
        int slot = 0;
        while (slot < getTupleCount() && getDataValue(slot).compareTo(searchKey) < 0) {
            slot++;
        }
        return slot - 1;
    }

    public void close() {
        if (currentBlock != null) {
            tx.unpin(currentBlock);
        }
        currentBlock = null;
    }

    public boolean isFull() {
        return getSlotPosition(getTupleCount() + 1) >= tx.getBlockSize();
    }

    /**
     * Given a position to split a block and a flag, split the block
     * into two parts, copying the tuples from the split position till the end
     * to the new block, and set the specified flag.
     */
    public BlockIdentifier split(int splitPosition, int flag) {
        /* Create a new formatted block and append it to the file */
        BlockIdentifier newBlock = appendNewBlock(flag);
        /* Create a new B-Tree page */
        BTreePage newBTPage = new BTreePage(tx, newBlock, layout);
        /* Move all the tuples from the split position till the end
         * of the page to the new page
         */
        transferTuples(splitPosition, newBTPage);
        /* Set the specified flag on the new page */
        newBTPage.setFlag(flag);
        /* Unpin the new block (hence the B-Tree page as well) */
        newBTPage.close();
        return newBlock;
    }

    public int getFlag() {
        return tx.getInt(currentBlock, 0);
    }

    public void setFlag(int flag) {
        tx.setInt(flag, currentBlock, 0, true);
    }

    public int getTupleCount() {
        return tx.getInt(currentBlock, Integer.BYTES);
    }

    /**
     * Get the block number where the slot is located
     */
    public int getBlockNumber(int slot) {
        return getInt(slot, "block");
    }

    public void insertDirectory(int slot, DataField value, int block) {
        makeRoomForNewSlot(slot);
        setValue(slot, "dataval", value);
        setInt(slot, "block", block);
    }

    public TupleIdentifier getTupleID(int slot) {
        return new TupleIdentifier(getInt(slot, "block"), getInt(slot, "id"));
    }

    public void insertLeaf(int slot, DataField value, TupleIdentifier tupleId) {
        makeRoomForNewSlot(slot);
        setValue(slot, "dataval", value);
        setInt(slot, "block", tupleId.getBlock());
        setInt(slot, "id", tupleId.getSlot());
    }

    private void transferTuples(int slot, BTreePage newPage) {
        /* This slot iterates through the slots in the new B-Tree
         * page
         */
        int destinationSlot = 0;
        while (slot < getTupleCount()) {
            /* Make room for the new slot to be inserted,
             * effectively shifting all tuples after the specified
             * slot right by one slot.
             */
            newPage.makeRoomForNewSlot(destinationSlot);
            TupleSchema schema = layout.getSchema();
            for (String fieldName: schema.getFields()) {
                /* Set the values of the old slot to the new destination slot */
                newPage.setValue(destinationSlot, fieldName, getValue(slot, fieldName));
            }
            /* Delete the old slot */
            delete(slot);
            /* Move to the next slot in the new page */
            destinationSlot++;
        }
    }

    public void delete(int slot) {
        for (int i = slot + 1; i < getTupleCount(); i++) {
            copyRecord(i, i - 1);
        }
        setTupleCount(getTupleCount() - 1);
    }

    private void setValue(int destinationSlot, String fieldName, DataField value) {
        TupleDataType type = layout.getSchema().getType(fieldName);
        if (type.equals(TupleDataType.INTEGER)) {
            setInt(destinationSlot, fieldName, (int) value.getValue());
        } else {
            setString(destinationSlot, fieldName, value.getValue().toString());
        }
    }

    private void setString(int destinationSlot, String fieldName, String value) {
        int position = getFieldPosition(destinationSlot, fieldName);
        tx.setString(value, currentBlock, position, true);
    }

    private void setInt(int destinationSlot, String fieldName, int value) {
        int position = getFieldPosition(destinationSlot, fieldName);
        tx.setInt(value, currentBlock, position, true);
    }

    private void makeRoomForNewSlot(int slot) {
        /* Start from the end of the page, where the last tuple is.
         * Iterate from there till the specified slot backwards,
         * copying the contents of the previous record to the next
         */
        for (int i = getTupleCount(); i > slot; i--) {
            copyRecord(i - 1, i);
        }
        setTupleCount(getTupleCount() + 1);
    }

    private void copyRecord(int from, int to) {
        /* Copy all data present in `from` slot to the `to` slot */
        TupleSchema schema = layout.getSchema();
        for (String fieldName: schema.getFields()) {
            setValue(to, fieldName, getValue(from, fieldName));
        }
    }

    private void setTupleCount(int i) {
        tx.setInt(i, currentBlock, Integer.BYTES, true);
    }

    public BlockIdentifier appendNewBlock(int flag) {
        BlockIdentifier block = tx.append(currentBlock.getFileName());
        tx.pin(block);
        format(block, flag);
        return block;
    }

    public void format(BlockIdentifier block, int flag) {
        tx.setInt(flag, block, 0, false);
        tx.setInt(0, block, Integer.BYTES, false);
        int tupleSize = layout.getSlotSize();
        for (int pos = 2 * Integer.BYTES; pos + tupleSize <= tx.getBlockSize(); pos += tupleSize) {
            makeDefaultTuple(block, pos);
        }
    }

    private void makeDefaultTuple(BlockIdentifier block, int pos) {
        for (String fieldName: layout.getSchema().getFields()) {
            int offset = layout.getOffset(fieldName);
            if (layout.getSchema().getType(fieldName).equals(TupleDataType.INTEGER)) {
                tx.setInt(0, block, pos + offset, false);
            } else {
                tx.setString("", block, pos + offset, false);
            }
        }
    }

    public DataField getDataValue(int slot) {
        return getValue(slot, "dataval");
    }

    public DataField getValue(int slot, String fieldName) {
        TupleSchema schema = layout.getSchema();
        TupleDataType type = schema.getType(fieldName);
        if (type.equals(TupleDataType.INTEGER)) {
            return new IntegerField(getInt(slot, fieldName));
        } else {
            int stringSize = schema.getField(fieldName).getSize();
            return new VarStringField(getString(slot, fieldName), stringSize);
        }
    }

    private int getInt(int slot, String fieldName) {
        int fieldPosition = getFieldPosition(slot, fieldName);
        return tx.getInt(currentBlock, fieldPosition);
    }

    private String getString(int slot, String fieldName) {
        int fieldPosition = getFieldPosition(slot, fieldName);
        return tx.getString(currentBlock, fieldPosition);
    }

    private int getFieldPosition(int slot, String fieldName) {
        int offset = layout.getOffset(fieldName);
        return getSlotPosition(slot) + offset;
    }

    private int getSlotPosition(int slot) {
        int slotSize = layout.getSlotSize();
        return Integer.BYTES + Integer.BYTES + (slot * slotSize);
    }
}
