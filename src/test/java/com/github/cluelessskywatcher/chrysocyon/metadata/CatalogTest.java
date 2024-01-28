package com.github.cluelessskywatcher.chrysocyon.metadata;

import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TableScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.TupleDataType;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

@TestInstance(Lifecycle.PER_CLASS)
public class CatalogTest {
    private Chrysocyon db;
    private ChrysoTransaction transaction;
    private TableManager mtdm;
    private TupleSchema schema1;

    @BeforeAll
    public void init() {
        db = new Chrysocyon("catalogtest", 400, 3);
        transaction = db.newTransaction();
        mtdm = new TableManager(true, transaction);
        schema1 = new TupleSchema();
        schema1.addField(new IntegerInfo(), "field1");
        schema1.addField(new VarStringInfo(20), "field2");
        schema1.addField(new IntegerInfo(), "field3");
        mtdm.createTable(transaction, "table1", schema1);
    }

    @Test
    public void testTableCatalog() {
        Map<String, Integer> slotSizes = Map.of(
            "schema_catalog", 32,
            "field_catalog", 64,
            "table1", 36
        );

        TupleLayout layout = mtdm.getLayout("schema_catalog", transaction);
        TableScan tScan = new TableScan(transaction, "schema_catalog", layout);
        
        while (tScan.next()) {
            String tableName = (String) tScan.getData("table_name").getValue();
            int size = (int) tScan.getData("slot_size").getValue();
            if (slotSizes.containsKey(tableName))
                Assertions.assertEquals(slotSizes.get(tableName), size);
        }
    }
    @Test
    public void testFieldCatalog() {
        TupleLayout fLayout = mtdm.getLayout("field_catalog", transaction);
        TableScan fScan = new TableScan(transaction, "field_catalog", fLayout);

        Map<String, Map<String, TupleDataType>> fieldTypes = Map.of(
            "schema_catalog", Map.of(
                "table_name", TupleDataType.VARSTR,
                "slot_size", TupleDataType.INTEGER
            ),
            "field_catalog", Map.of(
                "table_name", TupleDataType.VARSTR,
                "field_name", TupleDataType.VARSTR,
                "type", TupleDataType.INTEGER,
                "length", TupleDataType.INTEGER,
                "offset", TupleDataType.INTEGER
            ),
            "table1", Map.of(
                "field1", TupleDataType.INTEGER,
                "field2", TupleDataType.VARSTR,
                "field3", TupleDataType.INTEGER
            )
        );

        Map<String, Map<String, Integer>> fieldLengths = Map.of(
            "schema_catalog", Map.of(
                "table_name", 24,
                "slot_size", 4
            ),
            "field_catalog", Map.of(
                "table_name", 24,
                "field_name", 24,
                "type", 4,
                "length", 4,
                "offset", 4
            ),
            "table1", Map.of(
                "field1", 4,
                "field2", 24,
                "field3", 4
            )
        );

        Map<String, Map<String, Integer>> fieldOffsets = Map.of(
            "schema_catalog", Map.of(
                "table_name", 4,
                "slot_size", 28
            ),
            "field_catalog", Map.of(
                "table_name", 4,
                "field_name", 28,
                "type", 52,
                "length", 56,
                "offset", 60
            ),
            "table1", Map.of(
                "field1", 4,
                "field2", 8,
                "field3", 32
            )
        );

        while (fScan.next()) {
            String tableName = (String) fScan.getData("table_name").getValue();
            String fieldName = (String) fScan.getData("field_name").getValue();
            int type = (int) fScan.getData("type").getValue();
            int length = (int) fScan.getData("length").getValue();
            int offset = (int) fScan.getData("offset").getValue();

            if (fieldTypes.containsKey(tableName)) {
                Assertions.assertEquals(fieldTypes.get(tableName).get(fieldName).getCode(), type);
            }
            if (fieldLengths.containsKey(tableName)) {
                Assertions.assertEquals(fieldLengths.get(tableName).get(fieldName), length);
            }
            if (fieldOffsets.containsKey(tableName)) {
                Assertions.assertEquals(fieldOffsets.get(tableName).get(fieldName), offset);
            }
        }
    }

    @AfterAll
    public void shutDown() {
        db.nonStaticFactoryReset();
    }
}
