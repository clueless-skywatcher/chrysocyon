package com.github.cluelessskywatcher.chrysocyon.metadata;

import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

public class MetadataManager {
    private TableManager tableManager;
    private ViewManager viewManager;
    private StatisticsManager statsManager;
    private TableIndexManager indexManager;

    public MetadataManager(boolean isNew, ChrysoTransaction tx) {
        tableManager = new TableManager(isNew, tx);
        viewManager = new ViewManager(tableManager, tx);
        statsManager = new StatisticsManager(tableManager, tx);
        indexManager = new TableIndexManager(tableManager, statsManager, tx);
    }

    public void createTable(String tableName, TupleSchema schema, ChrysoTransaction tx) {
        tableManager.createTable(tx, tableName, schema);
    }

    public TupleLayout getLayout(String tableName, ChrysoTransaction tx) {
        return tableManager.getLayout(tableName, tx);
    }

    public void createView(String viewName, String viewDefinition, ChrysoTransaction tx) {
        viewManager.createView(viewName, viewDefinition, tx);
    }

    public String getViewDefinition(String viewName, ChrysoTransaction tx) {
        return viewManager.getViewDefinition(viewName, tx);
    }

    public void createIndex(String indexName, String tableName, String fieldName, ChrysoTransaction tx) {
        indexManager.createIndex(indexName, tableName, fieldName, tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tableName, ChrysoTransaction tx) {
        return indexManager.getIndexInfo(tableName, tx);
    }

    public StatisticalInfo getStatsInfo(String tableName, TupleLayout layout, ChrysoTransaction tx) {
        return statsManager.getStatisticalInfo(tableName, layout, tx);
    }
}
