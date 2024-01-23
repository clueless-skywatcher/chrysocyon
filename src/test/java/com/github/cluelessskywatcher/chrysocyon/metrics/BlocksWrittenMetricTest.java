package com.github.cluelessskywatcher.chrysocyon.metrics;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.ChrysoFileManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

public class BlocksWrittenMetricTest {
    private MetricFactory metricFactory = new MetricFactory();

    @Test
    public void testBlocksRead() {
        Chrysocyon kuon = Chrysocyon.getInstance();
        ChrysoFileManager fileManager = kuon.getFileManager();

        BlockIdentifier block = new BlockIdentifier("file1.dat", 2);

        PageObject p1 = new PageObject(fileManager.getBlockSize());
        int position1 = 69;
        String toBeStored = "abcdefgh";
        p1.setString(toBeStored, position1);
        int stringSize = PageObject.maxStringLength(toBeStored.length());
        int position2 = position1 + stringSize;
        p1.setInt(420, position2);
        fileManager.writeBlock(block, p1);
        Assertions.assertEquals(metricFactory.getMetricStats("filesystem.blockswritten"), 1);

        PageObject p2 = new PageObject(fileManager.getBlockSize());
        int position3 = 666;
        p2.setString("nemleria", position3);
        fileManager.writeBlock(block, p2);
        Assertions.assertEquals(metricFactory.getMetricStats("filesystem.blockswritten"), 2);
    }


    @AfterAll
    public static void shutDown() {
        Chrysocyon.factoryReset();
    }
}
