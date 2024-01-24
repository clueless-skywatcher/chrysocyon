package com.github.cluelessskywatcher.chrysocyon.transactions;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.buffer.BufferPoolManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.ChrysoFileManager;

public class ConcurrencyRun {
    private static ChrysoFileManager fileManager;
    private static AppendLogManager logManager;
    private static BufferPoolManager bufferManager;

    /**
     * Testing the concurrency of 3 transactions trying to work on 2 blocks
     * The 3 transactions are described as follows:
     * T1:  - Attempts a S-lock on Block 1 
     *      - Reads the value at Block 1
     *      - Releases the S-lock on Block 1
     *      - Attempts a S-lock on Block 2
     *      - Reads the value at Block 2
     *      - Release the S-lock on Block 2
     *      - Commits
     * 
     * T2:  - Attempts an X-lock on Block 2
     *      - Changes a value on Block 2
     *      - Releases the X-lock on Block 2
     *      - Attempts a S-lock on Block 1
     *      - Reads the value on Block 1
     *      - Releases the S-lock on Block 1
     *      - Commits
     * 
     * T3:  - Attempts an X-lock on Block 1
     *      - Changes a value on Block 1
     *      - Releases the X-lock on Block 1
     *      - Attempts a S-lock on Block 2
     *      - Reads the value on Block 2
     *      - Releases the S-lock on Block 2
     *      - Commits
     * 
     * One of the possible sequence of operations will be explained by a story:
     * - A, B and C are 3 people who want to use 2 pieces of paper for reading and writing. A manager 
     * - grants access to those papers as per their requirements, but he has to follow some rules:
     * - The 1st rule is: Any number of people can read from a piece of paper simultaneously
     * - The 2nd rule is: When one person is writing on a paper, other people cannot read from or write to it.
     *                    Other people must wait before the person has finished writing on the paper.
     * - The 3rd rule is: When one person is reading from a paper, other people cannot write to it.
     *                    Other people must wait before the person has finished reading the paper.
     * - A comes. A: Hey I want to read paper 1.
     * - Manager: Cool, go ahead.
     * - C comes. C: Hey I want to write to paper 1.
     * - Manager: I'm sorry you must wait for A to finish before you can write. Please wait
     * - B comes. B: I want to write to paper 2.
     * - Manager: Sure, nobody's using it. Go ahead.
     * - B writes 10 on the paper.
     * - A reads 0 from paper 1.
     * - B: I want to read paper 1.
     * - Manager: Sure, any number of people can read from paper 1 at a time
     * - A: I want to read paper 2 now.
     * - Manager: No please wait. B is writing there.
     * - A: But didn't B finish writing?
     * - Manager: Until we are fully convinced B doesn't need to write anymore I cannot
     * -          allow you to read now.
     * - B: Reads 0 from paper 1
     * - B: I think I'm done. Bye everyone (Transaction B commits)
     * - Manager: B has finished his tasks. A, please go ahead with paper 2
     * - A reads 10 from paper 2
     * - A: I am also done. Bye and thank you Mr. Manager (Transaction A commits)
     * - Manager: With pleasure, A. (Manager goes to C) Sorry for the wait, C. A left 
     *            so you can write on Paper 1 now
     * - C: Thanks, Manager.
     * - C writes 10 on paper 1
     * - C: I want to read paper 2 now
     * - Manager: Go ahead. Nobody's writing on it
     * - C reads 10 from paper 2
     * - C: Cool, that's enough for today. Bye Manager (Transaction C commits)
     * - Manager bids adieu to C and closes the building where they were working
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        Chrysocyon db = new Chrysocyon("concurrencytest", 1024, 8);
        
        fileManager = db.getFileManager();
        logManager = db.getLogManager();
        bufferManager = db.getBufferPoolManager();

        Run1 t1 = new Run1();
        Thread thread1 = new Thread(t1);
        thread1.start();
        
        Run2 t2 = new Run2();
        Thread thread2 = new Thread(t2);
        thread2.start();
        
        Run3 t3 = new Run3();
        Thread thread3 = new Thread(t3);
        thread3.start();
        
        thread1.join();
        thread2.join();
        thread3.join();

        db.nonStaticFactoryReset();
    }

    static class Run1 implements Runnable {
        @Override
        public void run() {
            try {
                ChrysoTransaction transaction1 = new ChrysoTransaction(fileManager, logManager, bufferManager);
                BlockIdentifier block1 = new BlockIdentifier("testfile", 1);
                BlockIdentifier block2 = new BlockIdentifier("testfile", 2);
                
                transaction1.pin(block1);
                transaction1.pin(block2);
                
                System.out.println("Transaction 1 : Requesting Shared lock on block 1");
                System.out.println("Transaction 1 : Read value: " + transaction1.getInt(block1, 0) + " from block 1");
                System.out.println("Transaction 1 : Received Shared lock on block 1");

                Thread.sleep(1000);

                System.out.println("Transaction 1 : Requesting Shared lock on block 2");
                System.out.println("Transaction 1 : Read value: " + transaction1.getInt(block2, 0) + " from block 2");
                System.out.println("Transaction 1 : Received Shared lock on block 2");

                transaction1.commit();
            }
            catch (InterruptedException e) {}
        }
        
    }

    static class Run2 implements Runnable {
        @Override
        public void run() {
            try {
                ChrysoTransaction transaction2 = new ChrysoTransaction(fileManager, logManager, bufferManager);
                BlockIdentifier block1 = new BlockIdentifier("testfile", 1);
                BlockIdentifier block2 = new BlockIdentifier("testfile", 2);

                transaction2.pin(block1);
                transaction2.pin(block2);

                System.out.println("Transaction 2 : Requesting Exclusive lock on block 2");
                transaction2.setInt(10, block2, 0, false);
                System.out.println("Transaction 2 : Setting value 10 on block 2 offset 0");
                System.out.println("Transaction 2 : Received Exclusive lock on block 2");

                Thread.sleep(1000);

                System.out.println("Transaction 2 : Requesting Shared lock on block 1");
                System.out.println("Transaction 2 : Read value: " + transaction2.getInt(block1, 0) + " from block 1");
                System.out.println("Transaction 2 : Received Shared lock on block 1");

                transaction2.commit();
            }
            catch (InterruptedException e) {}
        }
        
    }

    static class Run3 implements Runnable {
        @Override
        public void run() {
            try {
                ChrysoTransaction transaction3 = new ChrysoTransaction(fileManager, logManager, bufferManager);
                BlockIdentifier block1 = new BlockIdentifier("testfile", 1);
                BlockIdentifier block2 = new BlockIdentifier("testfile", 2);

                transaction3.pin(block1);
                transaction3.pin(block2);

                System.out.println("Transaction 3 : Requesting Exclusive lock on block 1");
                transaction3.setInt(10, block1, 0, false);
                System.out.println("Transaction 3 : Setting value 10 on block 1 offset 0");
                System.out.println("Transaction 3 : Received Exclusive lock on block 1");

                Thread.sleep(1000);

                System.out.println("Transaction 3 : Requesting Shared lock on block 2");
                System.out.println("Transaction 3 : Read value: " + transaction3.getInt(block2, 0) + " from block 2");
                System.out.println("Transaction 3 : Received Shared lock on block 2");

                transaction3.commit();
            }
            catch (InterruptedException e) {}
        }
        
    }
}
