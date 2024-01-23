package com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.Getter;

public class RollbackTransactionLogRecord implements RecoveryLogRecord {
    private @Getter int transaction;

    public RollbackTransactionLogRecord(PageObject p) {
        int transactionPosition = Integer.BYTES;
        this.transaction = p.getInt(transactionPosition);
    }

    @Override
    public RecoveryLogRecordType type() {
        return RecoveryLogRecordType.ROLLBACK_TRANSACTION;
    }

    public static int writeToLog(AppendLogManager logManager, int transaction) {
        int recordSize = 2 * Integer.BYTES;

        int transactionOffset = Integer.BYTES;
        
        byte[] logRecord = new byte[recordSize];

        PageObject p = new PageObject(logRecord);
        p.setInt(RecoveryLogRecordType.ROLLBACK_TRANSACTION.getCode(), 0);
        p.setInt(transaction, transactionOffset);
        return logManager.append(logRecord);
    }

    @Override
    public void undo(ChrysoTransaction txn) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'undo'");
    }
}
