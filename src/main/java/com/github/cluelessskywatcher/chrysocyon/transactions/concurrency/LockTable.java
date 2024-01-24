package com.github.cluelessskywatcher.chrysocyon.transactions.concurrency;

import java.util.HashMap;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.transactions.concurrency.exceptions.LockAbortedException;

public class LockTable {
    /*
     * A map that maps blocks to lock counts. A lock count of -1 denotes
     * that the block is locked by an X-lock and no transactions can acquire
     * any lock on it. Any other lock count denotes the number of S-locks on
     * it
     */
    private Map<BlockIdentifier, Integer> locks = new HashMap<>();

    /*
     * A transaction will only wait for 10 seconds to acquire a lock.
     */
    private static final long MAX_LOCK_WAITING_TIME = 10000;

    /**
     * Attempts to acquire a shared lock (S-lock) on a given block. Throws an exception
     * if an exclusive lock (X-lock) is already held on the block for too long, or the process 
     * is interrupted
     * @param block The block identifier to put an S-lock on
     */
    public synchronized void sharedLock(BlockIdentifier block) {
        try {
            /*
             * First we get the current timestamp.
             */
            long timestamp = System.currentTimeMillis();
            /*
             * If the block has an exclusive lock by some other transaction and our 
             * wait time has not exceeded, keep waiting
             */
            while (hasExclusiveLock(block) && !waitTimeExceeded(timestamp)) {
                wait(MAX_LOCK_WAITING_TIME);    
            }
            /*
             * Even after waiting for long enough if there is an exclusive lock
             * abort the mission
             */
            if (hasExclusiveLock(block)) {
                throw new LockAbortedException("Failed to gain shared lock due to exclusive lock on block " + block.toString());
            }
            
            int lockCount = getLockCount(block);
             /*
            * Update the lock count
            */
            locks.put(block, lockCount + 1);
        }
        catch (InterruptedException e) {
            throw new LockAbortedException("Interrupted acquisition of shared lock");
        }
    }

    private int getLockCount(BlockIdentifier block) {
        Integer lockCount = locks.get(block);
        return (lockCount == null) ? 0 : lockCount.intValue();
    }

    /**
     * Attempts to acquire an X-lock on a block. Throws an exception
     * if an exclusive lock (X-lock) is already held on the block for too long, 
     * or the process is interrupted
     * @param block The block identifier to put an X-lock on
     */
    public synchronized void exclusiveLock(BlockIdentifier block) {
        try {
            long timestamp = System.currentTimeMillis();

            /*
             * If the block has an exclusive lock by some transaction and our 
             * wait time has not exceeded, keep waiting
             */
            while (hasOtherSharedLocks(block) && !waitTimeExceeded(timestamp)) {
                wait(MAX_LOCK_WAITING_TIME);    
            }
            /*
             * Even after waiting for long enough if there is an exclusive lock
             * abort the mission
             */
            if (hasOtherSharedLocks(block)) {
                throw new LockAbortedException("Failed to gain shared lock due to exclusive lock on block " + block.toString());
            }
            /*
             * Update the lock count to -1 to denote that an exclusive lock is set
             */
            locks.put(block, -1);
        }
        catch (InterruptedException e) {
            throw new LockAbortedException("Interrupted acquisition of exclusive lock");
        }
    }

    private boolean hasOtherSharedLocks(BlockIdentifier block) {
        return getLockCount(block) > 1;
    }

    public synchronized void unlock(BlockIdentifier block) {
        int lockCount = getLockCount(block);
        if (lockCount > 1) {
            locks.put(block, lockCount - 1);
        }
        else {
            locks.remove(block);
            notifyAll();
        }
    }

    private boolean waitTimeExceeded(long timestamp) {
        return System.currentTimeMillis() - timestamp > MAX_LOCK_WAITING_TIME;
    }

    private boolean hasExclusiveLock(BlockIdentifier block) {
        return getLockCount(block) < 0;
    }
}
