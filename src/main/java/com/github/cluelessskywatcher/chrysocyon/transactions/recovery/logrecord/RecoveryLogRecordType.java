package com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord;

import java.util.HashMap;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

import lombok.Getter;

public enum RecoveryLogRecordType {
    CHECKPOINT(0),
    START_TRANSACTION(1),
    COMMIT_TRANSACTION(2),
    ROLLBACK_TRANSACTION(3),
    SET_INTEGER(4),
    SET_STRING(5);

    private final @Getter int code;
    private static final Map<Integer, RecoveryLogRecordType> lookup = new HashMap<Integer, RecoveryLogRecordType>();

    static {
        for (RecoveryLogRecordType m : RecoveryLogRecordType.values()) {
            lookup.put(m.getCode(), m);
        }
    }

    private RecoveryLogRecordType(int code) {
        this.code = code;
    }

    public static RecoveryLogRecordType getRecordType(int code) {
        return lookup.get(code);
    }

    public static RecoveryLogRecord createRecord(byte[] bytes) {
        PageObject p = new PageObject(bytes);
        int type = p.getInt(0);

        switch (getRecordType(type)) {
            case CHECKPOINT:
                return new CheckpointLogRecord();
            case START_TRANSACTION:
                return new StartTransactionLogRecord(p);
            case COMMIT_TRANSACTION:
                return new CommitTransactionLogRecord(p);
            case ROLLBACK_TRANSACTION:
                return new RollbackTransactionLogRecord(p);
            case SET_INTEGER:
                return new SetIntegerLogRecord(p);
            case SET_STRING:
                return new SetStringLogRecord(p);
        
            default:
                return null;
        }
    }
}
