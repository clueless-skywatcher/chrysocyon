package com.github.cluelessskywatcher.chrysocyon.transactions.concurrency;

import java.util.HashMap;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;

public class ConcurrencyManager {
    private static LockTable lockTable = new LockTable();
    private Map<BlockIdentifier, String> locks = new HashMap<>();
    
    public void sharedLock(BlockIdentifier block) {
        if (locks.get(block) == null) {
            lockTable.sharedLock(block);
            locks.put(block, "S");
        }
    }

    public void exclusiveLock(BlockIdentifier block) {
        if (!hasExclusiveLock(block)) {
            sharedLock(block);
            lockTable.exclusiveLock(block);
            locks.put(block, "X");
        }
    }

    public void release(){
        for (BlockIdentifier block : locks.keySet()) {
            lockTable.unlock(block);
        }
        locks.clear();
    }

    private boolean hasExclusiveLock(BlockIdentifier block) {
        String lockType = locks.get(block);
        return (lockType != null) && lockType.equals("X"); 
    }
}
