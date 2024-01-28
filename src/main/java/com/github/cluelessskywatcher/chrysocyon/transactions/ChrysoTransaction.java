package com.github.cluelessskywatcher.chrysocyon.transactions;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.buffer.BufferObject;
import com.github.cluelessskywatcher.chrysocyon.buffer.BufferPoolManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.ChrysoFileManager;
import com.github.cluelessskywatcher.chrysocyon.transactions.concurrency.ConcurrencyManager;
import com.github.cluelessskywatcher.chrysocyon.transactions.recovery.RecoveryManager;

import lombok.Getter;

public class ChrysoTransaction {
    private static int nextTransactionId = 0;
    private static final int END_OF_FILE = -1;

    private @Getter ChrysoFileManager fileManager;
    private @Getter AppendLogManager logManager;
    private @Getter BufferPoolManager bufferPoolManager;
    
    private RecoveryManager recoveryManager;
    private ConcurrencyManager concurrencyManager;

    private BufferList buffs;
    private @Getter int transactionId;

    public ChrysoTransaction(ChrysoFileManager fileManager, AppendLogManager logManager, BufferPoolManager bufferPoolManager) {
        this.fileManager = fileManager;
        this.logManager = logManager;
        this.bufferPoolManager = bufferPoolManager;

        this.transactionId = getNextTransactionId();
        this.recoveryManager = new RecoveryManager(this, transactionId, logManager, bufferPoolManager);
        this.concurrencyManager = new ConcurrencyManager();

        this.buffs = new BufferList(bufferPoolManager);
    }

    public void commit() {
        recoveryManager.commit();
        concurrencyManager.release();
        buffs.unpinAll();
        // System.out.println(String.format("Transaction %d committed", transactionId));
    }

    public void rollback() {
        recoveryManager.rollback();
        concurrencyManager.release();
        buffs.unpinAll();
        // System.out.println(String.format("Transaction %d rolled back", transactionId));
    }
    public void recover() {
        bufferPoolManager.flushAllBuffers(transactionId);
        recoveryManager.recover();
    }

    private static synchronized int getNextTransactionId() {
        nextTransactionId++;
        return nextTransactionId;
    }

    public void pin(BlockIdentifier block) {
        buffs.pin(block);
    }

    public void unpin(BlockIdentifier block) {
        buffs.unpin(block);
    }

    public int getInt(BlockIdentifier block, int offset) {
        concurrencyManager.sharedLock(block);
        BufferObject buffer = buffs.getBuffer(block);
        return buffer.getPage().getInt(offset);
    }

    public String getString(BlockIdentifier block, int offset) {
        concurrencyManager.sharedLock(block);
        BufferObject buffer = buffs.getBuffer(block);
        return buffer.getPage().getString(offset);
    }

    public void setInt(int value, BlockIdentifier block, int offset, boolean doLog) {
        concurrencyManager.exclusiveLock(block);
        BufferObject buffer = buffs.getBuffer(block);
        int lsn = -1;
        if (doLog) {
            lsn = recoveryManager.setInteger(buffer, offset, value);
        }
        buffer.getPage().setInt(value, offset);
        buffer.setModified(transactionId, lsn);
    }

    public void setString(String value, BlockIdentifier block, int offset, boolean doLog) {
        concurrencyManager.exclusiveLock(block);
        BufferObject buffer = buffs.getBuffer(block);
        int lsn = -1;
        if (doLog) {
            lsn = recoveryManager.setString(buffer, offset, value);
        }
        buffer.getPage().setString(value, offset);
        buffer.setModified(transactionId, lsn);
    }

    public BlockIdentifier append(String fileName) {
        BlockIdentifier dummyBlock = new BlockIdentifier(fileName, END_OF_FILE);
        concurrencyManager.exclusiveLock(dummyBlock);
        return fileManager.appendToFile(fileName);
    }

    public int getSize(String filename) {
        BlockIdentifier dummyBlock = new BlockIdentifier(filename, END_OF_FILE);
        concurrencyManager.sharedLock(dummyBlock);
        return fileManager.length(filename);
    }

    public int getBlockSize() {
        return fileManager.getBlockSize();
    }

    public int getAvailableBuffers() {
        return bufferPoolManager.availableBuffers();
    }
}
