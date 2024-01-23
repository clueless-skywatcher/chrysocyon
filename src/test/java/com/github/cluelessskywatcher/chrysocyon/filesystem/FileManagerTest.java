package com.github.cluelessskywatcher.chrysocyon.filesystem;

import org.junit.jupiter.api.Test;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;

public class FileManagerTest {
    @Test
    public void testBlockReadWrite() {
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

        PageObject p2 = new PageObject(fileManager.getBlockSize());
        fileManager.readBlock(block, p2);

        Assertions.assertEquals(p2.getInt(position2), 420);
        Assertions.assertTrue(p2.getString(position1).equals(toBeStored));
    }

    @AfterAll
    public static void shutDown() {
        Chrysocyon.factoryReset();
    }
}
