package com.github.cluelessskywatcher.chrysocyon.transactions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.buffer.BufferObject;
import com.github.cluelessskywatcher.chrysocyon.buffer.BufferPoolManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;

public class BufferList {
    private Map<BlockIdentifier, BufferObject> bufferMap = new HashMap<>();
    private List<BlockIdentifier> pinnedBlocks = new ArrayList<>();
    private BufferPoolManager bufferManager;

    public BufferList(BufferPoolManager bufferManager) {
        this.bufferManager = bufferManager;
    }

    BufferObject getBuffer(BlockIdentifier block) {
        return bufferMap.get(block);
    }

    void pin(BlockIdentifier block) {
        BufferObject buffer = bufferManager.pinBuffer(block);
        pinnedBlocks.add(block);
        bufferMap.put(block, buffer);
    }
    
    void unpin(BlockIdentifier block) {
        BufferObject buffer = bufferMap.get(block);
        bufferManager.unpinBuffer(buffer);
        pinnedBlocks.remove(block);
        if (!pinnedBlocks.contains(block)) {
            bufferMap.remove(block);
        }
    }

    void unpinAll() {
        for (BlockIdentifier block : pinnedBlocks) {
            BufferObject buffer = bufferMap.get(block);
            bufferManager.unpinBuffer(buffer);
        }
        bufferMap.clear();
        pinnedBlocks.clear();
    }

}
