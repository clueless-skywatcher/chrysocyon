package com.github.cluelessskywatcher.chrysocyon.metadata;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.AbstractMetaTableParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.ViewCatalogParameters;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;

public class ViewManager {
    private TableManager tableManager;

    public ViewManager(boolean isNew, TableManager tableManager, ChrysoTransaction tx) {
        this.tableManager = tableManager;
    }

    public void createView(String viewName, String viewDefinition, ChrysoTransaction tx) {
        AbstractMetaTableParameters params = new ViewCatalogParameters(viewName, viewDefinition);
        tableManager.insertDataToMetaTable(tx, MetaTableEnum.VIEW_CATALOG, params);
    }

    public String getViewDefinition(String viewName, ChrysoTransaction tx) {
        MetaTableEnum mt = MetaTableEnum.VIEW_CATALOG;
        String result = null;
        TupleLayout layout = tableManager.getMetaTableLayout(mt, tx);
        TableScan vScan = new TableScan(tx, TableManager.getMetaTableName(mt), layout);
        while (vScan.next()) {
            if (vScan.getData("view_name").getValue().equals(viewName)) {
                result = (String) vScan.getData("view_def").getValue();
                break;
            }
        }
        vScan.close();
        return result;
    }
}
