package com.github.cluelessskywatcher.chrysocyon.transactions.recovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.buffer.BufferObject;
import com.github.cluelessskywatcher.chrysocyon.buffer.BufferPoolManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord.CheckpointLogRecord;
import com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord.CommitTransactionLogRecord;
import com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord.RecoveryLogRecord;
import com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord.RecoveryLogRecordType;
import com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord.RollbackTransactionLogRecord;
import com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord.SetIntegerLogRecord;
import com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord.SetStringLogRecord;
import com.github.cluelessskywatcher.chrysocyon.transactions.recovery.logrecord.StartTransactionLogRecord;

import lombok.Getter;

public class RecoveryManager {
    private @Getter AppendLogManager logManager;
    private @Getter ChrysoTransaction txn;
    private @Getter BufferPoolManager bufferPoolManager;
    private @Getter int txNum;

    public RecoveryManager(ChrysoTransaction txn, int txNum, AppendLogManager logManager, BufferPoolManager bufferPoolManager) {
        this.logManager = logManager;
        this.txn = txn;
        this.bufferPoolManager = bufferPoolManager;
        this.txNum = txNum;
        StartTransactionLogRecord.writeToLog(logManager, txNum);
    }

    public void commit() {
        bufferPoolManager.flushAllBuffers(txNum);
        int lastLogNo = CommitTransactionLogRecord.writeToLog(logManager, txNum);
        logManager.flushToFile(lastLogNo);
    }

    private void performRecover() {
        Collection<Integer> finishedTransactions = new ArrayList<>();
        Iterator<byte []> logIterator = logManager.iterator();
        while (logIterator.hasNext()) {
            byte[] record = logIterator.next();
            RecoveryLogRecord logRecord = RecoveryLogRecordType.createRecord(record);
            if (logRecord.type() == RecoveryLogRecordType.CHECKPOINT) {
                return;
            }
            if (logRecord.type() == RecoveryLogRecordType.COMMIT_TRANSACTION || logRecord.type() == RecoveryLogRecordType.ROLLBACK_TRANSACTION) {
                finishedTransactions.add(logRecord.getTransaction());
            }
            else if (!finishedTransactions.contains(logRecord.getTransaction())) {
                logRecord.undo(txn);
            }
        }
    }


    public void recover() {
        performRecover();
        bufferPoolManager.flushAllBuffers(txNum);
        int lastLogNo = CheckpointLogRecord.writeToLog(logManager);
        logManager.flushToFile(lastLogNo);
    }

    private void performRollback() {
        Iterator<byte []> logIterator = logManager.iterator();
        while (logIterator.hasNext()) {
            byte[] record = logIterator.next();
            RecoveryLogRecord logRecord = RecoveryLogRecordType.createRecord(record);
            if (logRecord.getTransaction() == txNum) {
                if (logRecord.type() == RecoveryLogRecordType.START_TRANSACTION) {
                    return;
                }
                logRecord.undo(txn);
            }
        }
    }

    public void rollback() {
        performRollback();
        bufferPoolManager.flushAllBuffers(txNum);
        int lastLogNo = RollbackTransactionLogRecord.writeToLog(logManager, txNum);
        logManager.flushToFile(lastLogNo);
    }

    public int setInteger(BufferObject buffer, int offset, int newValue) {
        int oldValue = buffer.getPage().getInt(offset);
        BlockIdentifier block = buffer.getBlock();
        return SetIntegerLogRecord.writeToLog(logManager, txNum, block, offset, oldValue);
    }

    public int setString(BufferObject buffer, int offset, String newValue) {
        String oldValue = buffer.getPage().getString(offset);
        BlockIdentifier block = buffer.getBlock();
        return SetStringLogRecord.writeToLog(logManager, txNum, block, offset, oldValue);
    }

}
