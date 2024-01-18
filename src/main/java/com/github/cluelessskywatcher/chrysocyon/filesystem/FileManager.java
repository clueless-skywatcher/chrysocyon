package com.github.cluelessskywatcher.chrysocyon.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

public class FileManager {
    private @Getter File directory;
    private @Getter int blockSize;
    private @Getter boolean isNew;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();
    private @Getter int blocksRead = 0;
    private @Getter int blocksWritten = 0;

    public FileManager(File directory, int blockSize) {
        this.directory = directory;
        this.blockSize = blockSize;
        isNew = !directory.exists();

        if (isNew) {
            directory.mkdirs();
        }

        for (String file : directory.list()) {
            if (file.startsWith("temp")) {
                new File(directory, file).delete();
            }
        }
    }

    public synchronized void readBlock(BlockIdentifier block, PageObject p) {
        try {
            RandomAccessFile raf = getFile(block.getFileName());
            raf.seek(block.getBlockNumber() * blockSize);
            raf.getChannel().read(p.getContentBuffer());

            blocksRead++;
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read from block " + block.toString());
        }
    }

    public synchronized void writeBlock(BlockIdentifier block, PageObject p) {
        try {
            RandomAccessFile raf = getFile(block.getFileName());
            raf.seek(block.getBlockNumber() * blockSize);
            raf.getChannel().write(p.getContentBuffer());

            blocksWritten++;
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to write to block " + block.toString());
        }
    }

    public synchronized BlockIdentifier appendToFile(String fileName) {
        int newBlockNumber = length(fileName);
        BlockIdentifier block = new BlockIdentifier(fileName, newBlockNumber);
        byte[] b = new byte[blockSize];

        try {
            RandomAccessFile raf = getFile(block.getFileName());
            raf.seek(block.getBlockNumber() * blockSize);
            raf.write(b);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to append " + block.toString());
        }
        
        return block;
    }

    public int length(String fileName) {
        try {
            RandomAccessFile raf = getFile(fileName);
            return (int) (raf.length() / blockSize);
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to access file " + fileName);
        }
    }

    private RandomAccessFile getFile(String fileName) throws IOException {
        RandomAccessFile raf = openFiles.get(fileName);
        if (raf == null) {
            File table = new File(directory, fileName);
            raf = new RandomAccessFile(table, "rws");
            openFiles.put(fileName, raf);
        }
        return raf;
    }

}
