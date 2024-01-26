package com.github.cluelessskywatcher.chrysocyon.appendlog;

import java.util.Iterator;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.ChrysoFileManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

import lombok.Getter;

public class AppendLogManager {
    private @Getter ChrysoFileManager fileManager;
    private @Getter String logFile;
    private @Getter PageObject lastLogPage;
    private @Getter BlockIdentifier currentBlock;
    private @Getter int latestLogSeqNo = 0;
    private @Getter int lastSavedLogSeqNo = 0;

    /**
     * Class for managing log files that record database logs used
     * for rollback, commit and data updates 
     * @param fileManager
     * @param logFile
     */
    public AppendLogManager(ChrysoFileManager fileManager, String logFile) {
        this.fileManager = fileManager;
        this.logFile = logFile;

        byte[] b = new byte[fileManager.getBlockSize()];

        lastLogPage = new PageObject(b);

        int logSize = fileManager.length(logFile);
        
        if (logSize == 0) {
            currentBlock = appendNewBlock();
        }
        else {
            currentBlock = new BlockIdentifier(logFile, logSize - 1);
            fileManager.readBlock(currentBlock, lastLogPage);
        }
    }

    private BlockIdentifier appendNewBlock() {
        BlockIdentifier block = fileManager.appendToFile(logFile);
        lastLogPage.setInt(fileManager.getBlockSize(), 0);
        fileManager.writeBlock(block, lastLogPage);
        return block;
    }

    public void flushToFile(int lsn) {
        if (lsn >= lastSavedLogSeqNo) {
            flush();
        }
    }

    private void flush() {
        fileManager.writeBlock(currentBlock, lastLogPage);
        lastSavedLogSeqNo = latestLogSeqNo;
    }

    public synchronized int append(byte[] logRecord) {
        /*
         * Since I had a huge problem in understanding this, I need to take notes
         * and make it easier for the reader.
         * As per Edward Sciore's book, log records are inserted in each block
         * from RIGHT to LEFT to make it easy for the recovery manager to perform reads 
         * in the reverse order.
         * 
         * The initial 4 bytes of the page hold the "boundary" of the page,
         * the position where the latest log record was inserted. 
         * 
         * Let's say we have a record, then we will be inserting that record to the left of the 
         * boundary, i.e. we are aiming to insert the data at boundary - recordSize
         * position. 
         * 
         * If the recordSize is too large for us to be able to store the 
         * boundary position at the beginning of the log page, we need
         * to flush the current log page to disk, append a new page and set the boundary
         * of the page. 
         * 
         * Otherwise we just insert the record to the left of the boundary. 
         * Set the new boundary to the beginning position of the record we 
         * just inserted.
         */
        int boundary = lastLogPage.getInt(0);
        int recordSize = logRecord.length;

        int recordBytes = recordSize + Integer.BYTES;

        if (boundary - recordBytes < Integer.BYTES) {
            flush();
            currentBlock = appendNewBlock();
            boundary = lastLogPage.getInt(0);
        }

        int recordPosition = boundary - recordBytes;
        lastLogPage.setBytes(logRecord, recordPosition);
        lastLogPage.setInt(recordPosition, 0);

        latestLogSeqNo++;

        return latestLogSeqNo;
    }

    public Iterator<byte []> iterator() {
        flush();
        return new AppendLogIterator(fileManager, currentBlock);
    }

}
