package com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.Getter;

public class SetStringLogRecord implements RecoveryLogRecord {
    private @Getter int transaction;
    private @Getter int offset;
    private @Getter String value;
    private @Getter BlockIdentifier block;

    public SetStringLogRecord(PageObject p) {
        // A record will be of the form
        // { SET_INTEGER, transaction number, file name, block number, block offset, string value }

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
        this.value = p.getString(valuePosition);
    }

    @Override
    public RecoveryLogRecordType type() {
        return RecoveryLogRecordType.SET_STRING;
    }
    
    @Override
    public void undo(ChrysoTransaction txn) {
        txn.pin(block);
        txn.setString(value, block, offset, false);
        txn.unpin(block);
    }

    public String toString() {
        return String.format(
            "{SET_STRING, %d, %s, %d, %d, %s}", 
            transaction,
            block.getFileName(),
            block.getBlockNumber(),
            offset,
            value
        );
    }

    public static int writeToLog(AppendLogManager logManager, int transaction, BlockIdentifier block, int offset, String value) {
        int transactionPosition = Integer.BYTES;
        int fileNamePosition = transactionPosition + Integer.BYTES;
        int blockNumberPosition = fileNamePosition + PageObject.maxStringLength(block.getFileName().length());
        int blockOffsetPosition = blockNumberPosition + Integer.BYTES;
        int valuePosition = blockOffsetPosition + Integer.BYTES;
        int recordSize = valuePosition + PageObject.maxStringLength(value.length());
        
        byte[] logRecord = new byte[recordSize];

        PageObject p = new PageObject(logRecord);
        p.setInt(RecoveryLogRecordType.SET_STRING.getCode(), 0);
        p.setInt(transaction, transactionPosition);
        p.setString(block.getFileName(), fileNamePosition);
        p.setInt(block.getBlockNumber(), blockNumberPosition);
        p.setInt(offset, blockOffsetPosition);
        p.setString(value, valuePosition);

        return logManager.append(logRecord);
    }
    
}
