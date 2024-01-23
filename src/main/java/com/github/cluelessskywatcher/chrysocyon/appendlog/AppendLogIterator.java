package com.github.cluelessskywatcher.chrysocyon.appendlog;

import java.util.Iterator;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.ChrysoFileManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

public class AppendLogIterator implements Iterator<byte []> {
    private ChrysoFileManager fileManager;
    private BlockIdentifier block;
    private PageObject page;
    private int currentPosition;
    private int boundary;

    public AppendLogIterator(ChrysoFileManager fileManager, BlockIdentifier block) {
        this.fileManager = fileManager;
        this.block = block;
        byte[] b = new byte[fileManager.getBlockSize()];
        page = new PageObject(b);
        moveTo(block);
    }

    @Override
    public boolean hasNext() {
        return currentPosition < fileManager.getBlockSize() || block.getBlockNumber() > 0;
    }

    @Override
    public byte[] next() {
        if (currentPosition == fileManager.getBlockSize()) {
            block = new BlockIdentifier(block.getFileName(), block.getBlockNumber() - 1);
            moveTo(block);
        }

        byte[] record = page.getBytes(currentPosition);
        currentPosition += Integer.BYTES + record.length;
        return record;
    }

    private void moveTo(BlockIdentifier toBlock) {
        fileManager.readBlock(toBlock, page);
        boundary = page.getInt(0);
        currentPosition = boundary;
    }
    
}
