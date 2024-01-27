package com.github.cluelessskywatcher.chrysocyon.filesystem;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TuplePage;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.TupleDataType;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

@TestInstance(Lifecycle.PER_CLASS)
public class MetadataManagerTest {
    private Chrysocyon db;
    private ChrysoTransaction transaction;
    private MetadataManager mtdm;

    @BeforeAll
    public void init() {
        db = new Chrysocyon("metadatamanagertest", 400, 3);
        transaction = db.newTransaction();
        mtdm = new MetadataManager(true, transaction);
    }

    @Test
    public void testCreateTable() {
        TupleSchema schema = new TupleSchema();
        schema.addField(new IntegerInfo(), "field1");
        schema.addField(new VarStringInfo(20), "field2");
        mtdm.createTable(transaction, "table1", schema);

        TupleLayout layout = mtdm.getLayout("table1", transaction);
        Assertions.assertEquals(32, layout.getSlotSize());
        Assertions.assertEquals(layout.getSchema().getFields(), List.of(
            "field1", "field2"
        ));
        Map<String, TupleDataType> map = Map.of(
            "field1", TupleDataType.INTEGER,
            "field2", TupleDataType.VARSTR
        );
        
        for (Map.Entry<String, TupleDataType> entry : map.entrySet()) {
            Assertions.assertEquals(entry.getValue(), layout.getSchema().getType(entry.getKey()));
        }
    }

    @AfterAll
    public void shutDown() {
        db.nonStaticFactoryReset();
    }
}
