package com.github.cluelessskywatcher.chrysocyon.metadata.metatables;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.ViewConstants;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.AbstractMetaTableParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.params.ViewCatalogParameters;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

public class ViewCatalogTable extends AbstractMetaTable {
    public ViewCatalogTable() {
        super();
    }

    @Override
    public void initMetaTable() {
        tableName = "view_catalog";
        schema = new TupleSchema();
        schema.addField(new VarStringInfo(ViewConstants.MAX_VIEW_NAME_SIZE), "view_name");
        schema.addField(new VarStringInfo(ViewConstants.MAX_VIEW_DEF_SIZE), "view_def");
        layout = new TupleLayout(schema);
    }

    @Override
    public void insertData(ChrysoTransaction txn, AbstractMetaTableParameters params) {
        ViewCatalogParameters vParams = (ViewCatalogParameters) params;
        
        TableScan vScan = new TableScan(txn, tableName, layout);
        vScan.setData("view_name", new VarStringField(vParams.getName(), ViewConstants.MAX_VIEW_NAME_SIZE));
        vScan.setData("view_def", new VarStringField(vParams.getDefinition(), ViewConstants.MAX_VIEW_DEF_SIZE));
        vScan.close();
    }
    
}
