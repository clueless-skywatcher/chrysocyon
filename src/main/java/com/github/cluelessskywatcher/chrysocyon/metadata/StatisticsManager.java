package com.github.cluelessskywatcher.chrysocyon.metadata;

import java.util.HashMap;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.StatisticsConstants;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;

public class StatisticsManager {
    private Map<String, StatisticalInfo> tableStats;
    private TableManager tableManager;
    private int numCalls;

    public StatisticsManager(TableManager tableManager, ChrysoTransaction tx) {
        this.tableManager = tableManager;
        refreshTableStatistics(tx);
    }

    public synchronized StatisticalInfo getStatisticalInfo(String tableName, TupleLayout layout, ChrysoTransaction tx) {
        numCalls++;
        if (numCalls > StatisticsConstants.CALLS_BEFORE_REFRESH) {
            refreshTableStatistics(tx);
        }
        StatisticalInfo info = tableStats.get(tableName);
        if (info == null) {
            info = calculateStats(tableName, layout, tx);
            tableStats.put(tableName, info);
        }
        return info;
    }

    private synchronized void refreshTableStatistics(ChrysoTransaction tx) {
        tableStats = new HashMap<>();
        numCalls = 0;
        TupleLayout tcLayout = tableManager.getMetaTableLayout(MetaTableEnum.SCHEMA_CATALOG, tx);
        TableScan tcScan = new TableScan(tx, TableManager.getMetaTableName(MetaTableEnum.SCHEMA_CATALOG), tcLayout);
        while (tcScan.next()) {
            String tableName = (String) tcScan.getData("table_name").getValue();
            TupleLayout layout = tableManager.getLayout(tableName, tx);
            StatisticalInfo info = calculateStats(tableName, layout, tx);
            tableStats.put(tableName, info);
        }
        tcScan.close();
    }

    private StatisticalInfo calculateStats(String tableName, TupleLayout layout, ChrysoTransaction tx) {
        int numRecords = 0;
        int numBlocksAccessed = 0;

        TableScan tScan = new TableScan(tx, tableName, layout);
        if (tableName.equals("table1")) {
            System.out.println();
        }
        while (tScan.next()) {
            numRecords++;
            numBlocksAccessed = tScan.currentTupleId().getBlock() + 1;
        }
        tScan.close();

        StatisticalInfo info = new StatisticalInfo(numBlocksAccessed, numRecords);
        return info;
    }
}
