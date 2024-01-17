package com.github.cluelessskywatcher.chrysocyon.filesystem;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

public class FileManager {
    private @Getter File directory;
    private @Getter int blockSize;
    private @Getter boolean isNew;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileManager(File directory, int blockSize) {
        this.directory = directory;
        this.blockSize = blockSize;
        isNew = !directory.exists();

        if (isNew) {
            directory.mkdirs();
        }

        for (String file : directory.list()) {
            
        }
    }
}
