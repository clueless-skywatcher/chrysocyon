package com.github.cluelessskywatcher.chrysocyon.metadata;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

@TestInstance(Lifecycle.PER_CLASS)
public class MetadataManagerTest {
    private Chrysocyon db;
    private ChrysoTransaction transaction;
    private MetadataManager mtdm;

    @BeforeAll
    public void init() {
        db = new Chrysocyon("metadatamanagertest");
        transaction = db.newTransaction();
        mtdm = new MetadataManager(true, transaction);
    }

    @Test
    public void testTableMetadata() {
        TupleSchema schema = new TupleSchema();
        schema.addField(new IntegerInfo(), "field1");
        schema.addField(new VarStringInfo(20), "field2");
        mtdm.createTable("table1", schema, transaction);

        TupleLayout layout = mtdm.getLayout("table1", transaction);
        int size = layout.getSlotSize();

        Assertions.assertEquals(32, size);

        TupleSchema schema2 = layout.getSchema();

        for (String field : schema2.getFields()) {
            Assertions.assertEquals(schema.getType(field), schema2.getType(field));
        }
    }

    @Test
    public void testStatisticsMetadata() {
        TupleLayout layout = mtdm.getLayout("table1", transaction);
        TableScan tScan = new TableScan(transaction, "table1", layout);

        for (int i = 0; i < 50; i++) {
            tScan.insert();
            int n = (int) Math.round(Math.random() * 50);
            tScan.setData("field1", new IntegerField(n));
            tScan.setData("field2", new VarStringField("record" + n, 20));
        }

        StatisticalInfo statsInfo = mtdm.getStatsInfo("table1", layout, transaction);
        System.out.println("B(table1) = " + statsInfo.getNumBlocksAccessed());
        System.out.println("R(table1) = " + statsInfo.getNumRecords());
        System.out.println("V(table1, A) = " + statsInfo.getDistinctValues("A"));
        System.out.println("V(table1, B) = " + statsInfo.getDistinctValues("B"));

        Assertions.assertEquals(17, statsInfo.getDistinctValues("A"));
        Assertions.assertEquals(17, statsInfo.getDistinctValues("B"));
    }

    @Test
    public void testViewMetadata() {
        String viewDefinition = "select B from table1 where A = 1";
        mtdm.createView("view1", viewDefinition, transaction);
        Assertions.assertEquals(viewDefinition, mtdm.getViewDefinition("view1", transaction));
    }

    @AfterAll
    public void shutDown() {
        db.nonStaticFactoryReset();
    }
}
