package com.github.cluelessskywatcher.chrysocyon.buffer.replacement;

import com.github.cluelessskywatcher.chrysocyon.buffer.BufferPoolManager;
import com.github.cluelessskywatcher.chrysocyon.buffer.BufferObject;

public class NaiveReplacementStrategy implements BufferReplacementStrategy {
    private BufferPoolManager bufferManager;

    public NaiveReplacementStrategy(BufferPoolManager manager) {
        this.bufferManager = manager;
    }

    @Override
    public BufferObject chooseBufferToReplace() {
        for (BufferObject buffer : bufferManager.getBufferPool()) {
            if (!buffer.isPinned()) {
                return buffer;
            }
        }
        return null;
    }
    
}
