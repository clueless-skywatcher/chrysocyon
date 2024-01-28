package com.github.cluelessskywatcher.chrysocyon.metadata;

import java.util.HashMap;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.AbstractMetaTableParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.IndexCatalogParameters;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;

public class TableIndexManager {
    private TupleLayout layout;
    private TableManager tableManager;
    private StatisticsManager statsManager;

    public TableIndexManager(TableManager tableManager, StatisticsManager statsManager, ChrysoTransaction tx) {
        this.tableManager = tableManager;
        this.statsManager = statsManager;
        layout = this.tableManager.getMetaTableLayout(MetaTableEnum.INDEX_CATALOG, tx);
    }

    public void createIndex(String indexName, String tableName, String fieldName, ChrysoTransaction tx) {
        AbstractMetaTableParameters params = new IndexCatalogParameters(indexName, tableName, fieldName);
        this.tableManager.insertDataToMetaTable(tx, MetaTableEnum.INDEX_CATALOG, params);
    }

    public Map<String, IndexInfo> getIndexInfo(String tableName, ChrysoTransaction tx) {
        Map<String, IndexInfo> info = new HashMap<>();

        TableScan tScan = new TableScan(tx, TableManager.getMetaTableName(MetaTableEnum.INDEX_CATALOG), layout);
        while (tScan.next()) {
            if (tScan.getData("table_name").getValue().equals(tableName)) {
                String indexName = (String) tScan.getData("index_name").getValue();
                String fieldName = (String) tScan.getData("field_name").getValue();
                TupleLayout tableLayout = this.tableManager.getLayout(tableName, tx);
                StatisticalInfo statsInfo = this.statsManager.getStatisticalInfo(tableName, tableLayout, tx);
                IndexInfo indexInfo = new IndexInfo(indexName, fieldName, tx, tableLayout.getSchema(), tableLayout, statsInfo);
                info.put(tableName, indexInfo);
            }
        }

        tScan.close();
        return info;
    }
}
