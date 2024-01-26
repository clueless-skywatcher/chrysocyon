package com.github.cluelessskywatcher.chrysocyon.buffer;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.buffer.exceptions.BufferPinAbortedException;
import com.github.cluelessskywatcher.chrysocyon.buffer.replacement.BufferReplacementStrategy;
import com.github.cluelessskywatcher.chrysocyon.buffer.replacement.ReplacementStrategyEnum;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.ChrysoFileManager;

import lombok.Getter;
import lombok.Setter;

public class BufferPoolManager {
    private @Getter BufferObject[] bufferPool;
    private int availableBufferCount = 0;
    private @Getter @Setter ReplacementStrategyEnum replacementStrategy;
    private static final long MAX_PINNING_WAIT_TIME = 10000;

    /**
     * Class to manage buffers that transactions use to read or write
     * data from or to local memory
     * @param fileManager The manager object that manages the files
     * @param logManager The manager object that manages logs
     * @param bufferCount Number of buffers that will be used.
     */
    public BufferPoolManager(ChrysoFileManager fileManager, AppendLogManager logManager, int bufferCount) {
        this.bufferPool = new BufferObject[bufferCount];
        this.availableBufferCount = bufferCount;
        for (int i = 0; i < bufferCount; i++) {
            this.bufferPool[i] = new BufferObject(fileManager, logManager);
        }
        this.replacementStrategy = ReplacementStrategyEnum.NAIVE;
    }

    public BufferPoolManager(ChrysoFileManager fileManager, AppendLogManager logManager, int bufferCount, ReplacementStrategyEnum strategy) {
        bufferPool = new BufferObject[bufferCount];
        availableBufferCount = bufferCount;
        for (int i = 0; i < bufferCount; i++) {
            bufferPool[i] = new BufferObject(fileManager, logManager);
        }
        this.replacementStrategy = strategy;
    }

    public synchronized int availableBuffers() {
        return availableBufferCount;
    }

    /**
     * Pins a buffer, i.e. marks the
     * @param block
     * @return
     */
    public synchronized BufferObject pinBuffer(BlockIdentifier block) {
        try {
            long timestamp = System.currentTimeMillis();
            BufferObject buffer = attemptPin(block);
            while (buffer == null && !waitTimeExceeded(timestamp)) {
                wait(MAX_PINNING_WAIT_TIME);
                buffer = attemptPin(block);
            }
            if (buffer == null) {
                throw new BufferPinAbortedException("Failed to acquire buffer");
            }
            return buffer;
        }
        catch (InterruptedException e) {
            throw new BufferPinAbortedException(e.getMessage());
        }
    }

    private boolean waitTimeExceeded(long timestamp) {
        return System.currentTimeMillis() - timestamp > MAX_PINNING_WAIT_TIME;
    }

    private BufferObject attemptPin(BlockIdentifier block) {
        BufferObject buffer = findBufferHoldingBlock(block);
        if (buffer == null) {
            BufferReplacementStrategy strategy = ReplacementStrategyEnum.getStrategy(this, replacementStrategy);
            buffer = strategy.chooseBufferToReplace();
            if (buffer == null) {
                return null;
            }
            buffer.assignToBlock(block);
        }

        if (!buffer.isPinned()) {
            availableBufferCount--;
        }
        buffer.pin();
        return buffer;
    }

    private BufferObject findBufferHoldingBlock(BlockIdentifier block) {
        for (BufferObject buffer : bufferPool) {
            BlockIdentifier bufferBlock = buffer.getBlock();
            if (bufferBlock != null && bufferBlock.equals(block)) {
                return buffer;
            }
        }
        return null;
    }

    public synchronized void unpinBuffer(BufferObject buffer) {
        buffer.unpin();   
        if (!buffer.isPinned()){
            availableBufferCount++;
            notifyAll();
        }     
    }

    public synchronized void flushAllBuffers(int transactionId) {
        for (BufferObject buffer : bufferPool) {
            if (buffer.getModifyingTransaction() == transactionId) {
                buffer.flush();
            }
        }
    }

}
