package com.github.cluelessskywatcher.chrysocyon;

import java.io.File;

import com.github.cluelessskywatcher.chrysocyon.filesystem.FileManager;

import lombok.Getter;

public class Chrysocyon {
    private static Chrysocyon instance = null;

    private @Getter FileManager fileManager;

    public Chrysocyon(String directory, int blockSize) {
        this.fileManager = new FileManager(new File(directory), blockSize);
    }

    public static Chrysocyon getInstance() {
        if (instance == null) {
            instance = new Chrysocyon(".chrysocyon", 1024);
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
