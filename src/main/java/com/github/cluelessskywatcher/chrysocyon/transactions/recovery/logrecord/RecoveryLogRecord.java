package com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord;

import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public interface RecoveryLogRecord {
    RecoveryLogRecordType type();

    public int getTransaction();
    
    void undo(ChrysoTransaction txn);
}
