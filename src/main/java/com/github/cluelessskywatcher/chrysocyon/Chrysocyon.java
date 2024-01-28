package com.github.cluelessskywatcher.chrysocyon;

import java.io.File;

import com.github.cluelessskywatcher.chrysocyon.appendlog.AppendLogManager;
import com.github.cluelessskywatcher.chrysocyon.buffer.BufferPoolManager;
import com.github.cluelessskywatcher.chrysocyon.filesystem.ChrysoFileManager;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.planning.BasicModifyPlanner;
import com.github.cluelessskywatcher.chrysocyon.planning.ChrySQLPlanner;
import com.github.cluelessskywatcher.chrysocyon.planning.GreedyQueryPlanner;
import com.github.cluelessskywatcher.chrysocyon.planning.ModifyPlanner;
import com.github.cluelessskywatcher.chrysocyon.planning.QueryPlanner;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.Getter;

public class Chrysocyon {
    private static Chrysocyon instance = null;
    private static final String HOME_DIR = "user.dir";
    private static final String LOG_FILE = "chryso.log";
    private static final int BUFFER_SIZE = 8;
    private static final int BLOCK_SIZE = 1024;

    private @Getter ChrysoFileManager fileManager;
    private @Getter AppendLogManager logManager;
    private @Getter BufferPoolManager bufferPoolManager;
    private @Getter MetadataManager metadataManager;
    private @Getter ChrySQLPlanner planner;

    public Chrysocyon(String dirName, int blockSize, int buffers) {
        String home = System.getProperty(HOME_DIR);
        File directory = new File(home, dirName);
        this.fileManager = new ChrysoFileManager(directory, blockSize);
        this.logManager = new AppendLogManager(fileManager, LOG_FILE);
        this.bufferPoolManager = new BufferPoolManager(fileManager, logManager, buffers);
    }

    public Chrysocyon(String dirName) {
        this(dirName, BLOCK_SIZE, BUFFER_SIZE);
        boolean isNew = fileManager.isNew();
        ChrysoTransaction transaction = newTransaction();
        this.metadataManager = new MetadataManager(isNew, transaction);

        QueryPlanner queryPlanner = new GreedyQueryPlanner(metadataManager);
        ModifyPlanner modifyPlanner = new BasicModifyPlanner(metadataManager);

        this.planner = new ChrySQLPlanner(queryPlanner, modifyPlanner);
        transaction.commit();
    }

    public static Chrysocyon getInstance() {
        if (instance == null) {
            instance = new Chrysocyon(".chrysocyon");
        }

        return instance;
    }

    public static void factoryReset() {
        clearAllFiles(instance.getFileManager().getDirectory());
        instance = null;
    }

    public void nonStaticFactoryReset() {
        clearAllFiles(this.fileManager.getDirectory());
    }

    private static void clearAllFiles(File dir) {
        File[] contents = dir.listFiles();
        if (contents != null) {
            for (File f : contents) {
                if (!f.isDirectory()) {
                    f.delete();
                } else {
                    clearAllFiles(f);
                }
            }
        }
        dir.delete();
    }

    public ChrysoTransaction newTransaction() {
        return new ChrysoTransaction(fileManager, logManager, bufferPoolManager);
    }
}
