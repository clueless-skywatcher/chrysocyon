package com.github.cluelessskywatcher.chrysocyon;

import java.io.File;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.buffer.BufferPoolManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.FileManager;

import lombok.Getter;

public class Chrysocyon {
    private static Chrysocyon instance = null;

    private @Getter FileManager fileManager;
    private @Getter AppendLogManager logManager;
    private @Getter BufferPoolManager bufferPoolManager;

    public Chrysocyon(String directory, int blockSize, int buffers) {
        this.fileManager = new FileManager(new File(directory), blockSize);
        this.logManager = new AppendLogManager(fileManager, directory);
        this.bufferPoolManager = new BufferPoolManager(fileManager, logManager, buffers);
    }

    public static Chrysocyon getInstance() {
        if (instance == null) {
            instance = new Chrysocyon(".chrysocyon", 1024, 3);
        }

        return instance;
    }

    public static void factoryReset() {
        clearAllFiles(instance.getFileManager().getDirectory());
        instance = null;
    }

    private static void clearAllFiles(File dir) {
        File[] contents = dir.listFiles();
        if (contents != null) {
            for (File f : contents) {
                clearAllFiles(f);
            }
        }
        dir.delete();
    }
}
