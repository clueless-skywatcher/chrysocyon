package com.github.cluelessskywatcher.chrysocyon.metrics;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.filesystem.FileManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.PageObject;

public class BlocksReadMetricTest {
    private MetricFactory metricFactory = new MetricFactory();

    @Test
    public void testBlocksRead() {
        Chrysocyon kuon = Chrysocyon.getInstance();
        FileManager fileManager = kuon.getFileManager();

        BlockIdentifier block = new BlockIdentifier("file1.dat", 2);

        PageObject p1 = new PageObject(fileManager.getBlockSize());
        int position1 = 69;
        String toBeStored = "abcdefgh";
        p1.setString(toBeStored, position1);
        int stringSize = PageObject.maxStringLength(toBeStored.length());
        int position2 = position1 + stringSize;
        p1.setInt(420, position2);
        fileManager.writeBlock(block, p1);

        PageObject p2 = new PageObject(fileManager.getBlockSize());
        fileManager.readBlock(block, p2);

        Assertions.assertEquals(metricFactory.getMetricStats("filesystem.blocksread"), 1);

        fileManager.readBlock(block, p1);

        Assertions.assertEquals(metricFactory.getMetricStats("filesystem.blocksread"), 2);
    }


    @AfterAll
    public static void shutDown() {
        Chrysocyon.factoryReset();
    }
}
