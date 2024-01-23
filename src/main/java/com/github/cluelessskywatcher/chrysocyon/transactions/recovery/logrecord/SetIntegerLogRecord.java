package com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.Getter;

public class SetIntegerLogRecord implements RecoveryLogRecord {
    private @Getter int transaction;
    private @Getter int offset;
    private @Getter int value;
    private @Getter BlockIdentifier block;

    public SetIntegerLogRecord(PageObject p) {
        // A record will be of the form
        // { SET_INTEGER, transaction number, file name, block number, block offset, integer value }

        int transactionPosition = Integer.BYTES;
        this.transaction = p.getInt(transactionPosition);
        
        int fileNamePosition = transactionPosition + Integer.BYTES;
        String fileName = p.getString(fileNamePosition);
        int blockNumberPosition = fileNamePosition + PageObject.maxStringLength(fileName.length());
        int blockNum = p.getInt(blockNumberPosition);
        this.block = new BlockIdentifier(fileName, blockNum);

        int blockOffsetPosition = blockNumberPosition + Integer.BYTES;
        this.offset = p.getInt(blockOffsetPosition);

        int valuePosition = blockOffsetPosition + Integer.BYTES;
        this.value = p.getInt(valuePosition);
    }

    @Override
    public RecoveryLogRecordType type() {
        return RecoveryLogRecordType.SET_INTEGER;
    }

    public String toString() {
        return String.format(
            "{SET_INTEGER, %d, %s, %d, %d, %d}", 
            transaction,
            block.getFileName(),
            block.getBlockNumber(),
            offset,
            value
        );
    }

    @Override
    public void undo(ChrysoTransaction txn) {
        txn.pin(block);
        txn.setInt(value, block, offset, false);
        txn.unpin(block);
    }

    public static int writeToLog(AppendLogManager logManager, int transaction, BlockIdentifier block, int offset, int value) {
        int transactionPosition = Integer.BYTES;
        int fileNamePosition = transactionPosition + Integer.BYTES;
        int blockNumberPosition = fileNamePosition + PageObject.maxStringLength(block.getFileName().length());
        int blockOffsetPosition = blockNumberPosition + Integer.BYTES;
        int valuePosition = blockOffsetPosition + Integer.BYTES;
        int recordSize = valuePosition + Integer.BYTES;
        
        byte[] logRecord = new byte[recordSize];

        PageObject p = new PageObject(logRecord);
        p.setInt(RecoveryLogRecordType.SET_INTEGER.getCode(), 0);
        p.setInt(transaction, transactionPosition);
        p.setString(block.getFileName(), fileNamePosition);
        p.setInt(block.getBlockNumber(), blockNumberPosition);
        p.setInt(offset, blockOffsetPosition);
        p.setInt(value, valuePosition);

        return logManager.append(logRecord);
    }
    
}
