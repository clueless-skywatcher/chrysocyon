package com.github.cluelessskywatcher.chrysocyon.tuples;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.TupleDataType;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

import lombok.Getter;

public class TuplePage {
    private static final int EMPTY = 0, USED = 1;

    private ChrysoTransaction transaction;
    private @Getter BlockIdentifier block;
    private TupleLayout layout;

    /**
     * Class for denoting a page of fixed-size unspanned tuples. Each
     * tuple has the following format:
     * - A flag to denote whether a slot in the page is used by a tuple or not
     * - THe actual data of the tuple
     * @param tx A transaction that handles operations with the record page
     * @param block A block of a file
     * @param layout The physical layout of each tuple. Stores the offsets where each tuple field is stored
     */
    public TuplePage(ChrysoTransaction tx, BlockIdentifier block, TupleLayout layout) {
        this.transaction = tx;
        this.block = block;
        this.layout = layout;
        // Need to pin the block in order to load it into transaction buffer
        tx.pin(block);
    }

    /**
     * Retrieves the data in a row slot specified by a given field name
     * @param slot The row slot number
     * @param fieldName The name of the field
     * @return
     */
    public DataField getData(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);

        switch (layout.getSchema().getType(fieldName)) {
            case INTEGER:
                return new IntegerField(transaction.getInt(block, fieldPosition));
            case VARSTR:
                int infoLength = ((VarStringInfo)(layout.getSchema().getField(fieldName))).getCharSize();
                return new VarStringField(transaction.getString(block, fieldPosition), infoLength);
            default:
                return null;
        }
    }

    /**
     * Sets the data specified by given field name in a given row slot
     * @param slot
     * @param fieldName
     * @param value
     */
    public void setData(int slot, String fieldName, DataField value) {
        int fieldPosition = offset(slot) + layout.getOffset(fieldName);

        switch (layout.getSchema().getType(fieldName)) {
            case INTEGER:
                transaction.setInt((Integer) value.getValue(), block, fieldPosition, true);
                break;
            case VARSTR:
                transaction.setString((String) value.getValue(), block, fieldPosition, true);
                break;
            default:
                return;
        }
    }

    private int offset(int slot) {
        return slot * layout.getSlotSize();
    }

    /**
     * Delete the row specified by the slot number
     * @param slot The slot number whose row is to be deleted
     */
    public void delete(int slot) {
        setFlag(slot, EMPTY);
    }

    private void setFlag(int slot, int flag) {
        transaction.setInt(flag, block, offset(slot), true);
    }

    /**
     * Formats the data and sets everything to default values
     * (0 for integers, "" for strings)
     */
    public void setDefaults() {
        int slot = 0;
        while (isValidSlot(slot)) {
            transaction.setInt(EMPTY, block, offset(slot), false);
            TupleSchema schema = layout.getSchema();
            for (String fieldName : schema.getFields()) {
                int fieldPosition = offset(slot) + layout.getOffset(fieldName);
                if (schema.getType(fieldName) == TupleDataType.INTEGER) {
                    transaction.setInt(0, block, fieldPosition, false);
                }
                else if (schema.getType(fieldName) == TupleDataType.VARSTR) {
                    transaction.setString("", block, fieldPosition, false);
                }
            }
            slot++;
        }
    }

    /**
     * Given a slot ID, get the next used slot ID if it exists
     * @param slot
     * @return the next used slot ID if it exists, or -1 if it doesn't
     */
    public int nextSlot(int slot) {
        return searchNext(slot, USED);
    }

    private int searchNext(int slot, int flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (transaction.getInt(block, offset(slot)) == flag) {
                return slot;
            }
            slot++;
        }
        return -1;
    }

    /**
     * Given a slot ID, get the next empty slot ID and mark it as being
     * used
     * @param slot
     * @return the next empty slot ID if it exists, or -1 if it doesn't. The returned
     * slot ID is marked as used
     */
    public int nextSlotToInsert(int slot) {
        int nextUnusedSlot = searchNext(slot, EMPTY);
        if (nextUnusedSlot >= 0) {
            setFlag(nextUnusedSlot, USED);
        }
        return nextUnusedSlot;
    }

    private boolean isValidSlot(int slot) {
        return offset(slot + 1) <= transaction.getBlockSize();
    }

}

