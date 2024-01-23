package com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public class CheckpointLogRecord implements RecoveryLogRecord {

    @Override
    public RecoveryLogRecordType type() {
        return RecoveryLogRecordType.CHECKPOINT;
    }

    public static int writeToLog(AppendLogManager logManager) {
        int recordSize = Integer.BYTES;
        
        byte[] logRecord = new byte[recordSize];

        PageObject p = new PageObject(logRecord);
        p.setInt(RecoveryLogRecordType.CHECKPOINT.getCode(), 0);
        return logManager.append(logRecord);
    }

    @Override
    public void undo(ChrysoTransaction txn) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'undo'");
    }

    @Override
    public int getTransaction() {
        return -1;
    }
}
