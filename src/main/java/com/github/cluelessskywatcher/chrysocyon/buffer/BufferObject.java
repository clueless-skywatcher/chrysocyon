package com.github.cluelessskywatcher.chrysocyon.buffer;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.FileManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

import lombok.Getter;

public class BufferObject{ 
    private @Getter FileManager fileManager;
    private @Getter AppendLogManager logManager;
    private @Getter BlockIdentifier block = null;
    private @Getter PageObject page;
    private @Getter int modifyingTransaction = -1;
    private @Getter int logSeqNo = -1;

    private int pins = 0;

    public BufferObject(FileManager fileManager, AppendLogManager logManager) {
        this.fileManager = fileManager;
        this.logManager = logManager;
        page = new PageObject(fileManager.getBlockSize());
    }

    public void setModified(int transactionId, int logSeqNo) {
        this.modifyingTransaction = transactionId;
        if (logSeqNo >= 0) this.logSeqNo = logSeqNo;
    }

    public boolean isPinned() {
        return pins > 0;
    }

    void assignToBlock(BlockIdentifier block) {
        flush();
        this.block = block;
        fileManager.readBlock(block, page);
        pins = 0;
    }

    void flush() {
        if (modifyingTransaction >= 0) {
            logManager.flushToFile(logSeqNo);
            fileManager.writeBlock(block, page);
            modifyingTransaction = -1;
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }
}
