package com.github.cluelessskywatcher.chrysocyon.metadata;

import java.util.HashMap;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.AbstractMetaTable;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.AbstractMetaTableParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.FieldCatalogParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.FieldCatalogTable;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.SchemaCatalogParameters;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.SchemaCatalogTable;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.TupleDataType;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

public class MetadataManager {
    private static Map<MetaTableEnum, AbstractMetaTable> metaTables = new HashMap<>();

    static {
        metaTables.put(MetaTableEnum.SCHEMA_CATALOG, new SchemaCatalogTable());
        metaTables.put(MetaTableEnum.FIELD_CATALOG, new FieldCatalogTable());
    }

    public MetadataManager(boolean isNew, ChrysoTransaction tx) {
        if (isNew) {
            for (AbstractMetaTable mt : metaTables.values()) {
                createTable(tx, mt.getTableName(), mt.getSchema());
            }
        }
    }
    
    public void createTable(ChrysoTransaction txn, String tableName, TupleSchema schema) {
        TupleLayout layout = new TupleLayout(schema);
        AbstractMetaTableParameters params = new SchemaCatalogParameters(tableName, layout);
        metaTables.get(MetaTableEnum.SCHEMA_CATALOG).insertData(txn, params);

        for (String field : schema.getFields()) {
            AbstractMetaTableParameters fcParams = new FieldCatalogParameters(
                tableName, field, 
                schema.getType(field).getCode(), 
                layout.getOffset(field),
                schema.length(field) 
            );
            metaTables.get(MetaTableEnum.FIELD_CATALOG).insertData(txn, fcParams);
        }
    }

    public TupleLayout getLayout(String tableName, ChrysoTransaction tx) {
        int size = -1;
        TableScan tCatScan = new TableScan(
            tx, 
            metaTables.get(MetaTableEnum.SCHEMA_CATALOG).getTableName(), 
            metaTables.get(MetaTableEnum.SCHEMA_CATALOG).getLayout()
        );

        while (tCatScan.next()) {
            if (tCatScan.getData("table_name").getValue().equals(tableName)) {
                size = (int) tCatScan.getData("slot_size").getValue();
                break;
            }
        }
        tCatScan.close();

        TupleSchema schema = new TupleSchema();
        Map<String, Integer> offsets = new HashMap<>();

        TableScan fCatScan = new TableScan(
            tx,
            metaTables.get(MetaTableEnum.FIELD_CATALOG).getTableName(), 
            metaTables.get(MetaTableEnum.FIELD_CATALOG).getLayout()
        );

        while (fCatScan.next()) {
            if (fCatScan.getData("table_name").getValue().equals(tableName)) {
                String fieldName = (String) fCatScan.getData("field_name").getValue();
                int type = (int) fCatScan.getData("type").getValue();
                int length = (int) fCatScan.getData("length").getValue();
                int offset = (int) fCatScan.getData("offset").getValue();
                offsets.put(fieldName, offset);
                if (type == TupleDataType.INTEGER.getCode()) {
                    schema.addField(new IntegerInfo(), fieldName);
                }
                else if (type == TupleDataType.VARSTR.getCode()) {
                    schema.addField(new VarStringInfo(length), fieldName);
                }
            }
        }
        fCatScan.close();
        return new TupleLayout(schema, offsets, size);
    }

}
