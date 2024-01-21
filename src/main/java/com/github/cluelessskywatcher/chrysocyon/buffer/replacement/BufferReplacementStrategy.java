package com.github.cluelessskywatcher.chrysocyon.buffer.replacement;

import com.github.cluelessskywatcher.chrysocyon.buffer.BufferObject;

public interface BufferReplacementStrategy {
    /*
     * This method chooses an unpinned buffer to be replaced
     * as per the replacement strategy.
     */
    public BufferObject chooseBufferToReplace();    
} 