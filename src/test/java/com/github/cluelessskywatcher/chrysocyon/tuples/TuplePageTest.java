package com.github.cluelessskywatcher.chrysocyon.tuples;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.filesystem.BlockIdentifier;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

@TestInstance(Lifecycle.PER_CLASS)
public class TuplePageTest {
    private static Chrysocyon db;
    private static TupleSchema schema;
    private static TupleLayout layout;
    private static ChrysoTransaction tx;

    @BeforeAll
    public void initialize() {
        db = new Chrysocyon("tuplepagetest", 400, 3);
        schema = new TupleSchema();
        schema.addField(new IntegerInfo(), "field1");
        schema.addField(new VarStringInfo(20), "field2");

        layout = new TupleLayout(schema);

        tx = db.newTransaction();
    }

    @Test
    public void testTuples() {
        BlockIdentifier block = tx.append("testfile");
        tx.pin(block);

        TuplePage page = new TuplePage(tx, block, layout);
        page.setDefaults();

        int slot = page.nextSlotToInsert(-1);

        List<Integer> fields1 = new ArrayList<>();
        List<String> fields2 = new ArrayList<>();

        Random random = new Random();
        // Filling up random values first
        while (slot >= 0) {
            int n = random.nextInt(100);
            fields1.add(n);
            String s = String.format("String%d", n);
            fields2.add(s);
            page.setData(slot, "field1", new IntegerField(n));
            page.setData(slot, "field2", new VarStringField(s, 20));
            slot = page.nextSlotToInsert(slot);
        }

        // Preparing to read and compare data
        slot = page.nextSlot(-1);
        
        List<Integer> dataRead1 = new ArrayList<>();
        List<String> dataRead2 = new ArrayList<>();

        while (slot >= 0) {
            int data1 = (int) page.getData(slot, "field1").getValue();
            String data2 = (String) page.getData(slot, "field2").getValue();
            dataRead1.add(data1);
            dataRead2.add(data2);

            slot = page.nextSlot(slot);
        }

        Assertions.assertEquals(fields1, dataRead1);
        Assertions.assertEquals(fields2, dataRead2);

        // Preparing to delete all values less than 25
        slot = page.nextSlot(-1);

        List<Integer> filteredFields1 = fields1.stream()
            .filter(entry -> entry >= 25)
            .collect(Collectors.toList())
            ;
        List<String> filteredFields2 = new ArrayList<>();
        
        filteredFields1.forEach((entry) -> filteredFields2.add(String.format("String%d", entry)));

        while (slot >= 0) {
            int data1 = (int) page.getData(slot, "field1").getValue();
            if (data1 < 25) {
                page.delete(slot);
            }
            slot = page.nextSlot(slot);
        }

        // Preparing to read again
        slot = page.nextSlot(-1);

        dataRead1 = new ArrayList<>();
        dataRead2 = new ArrayList<>();

        while (slot >= 0) {
            int data1 = (int) page.getData(slot, "field1").getValue();
            String data2 = (String) page.getData(slot, "field2").getValue();
            dataRead1.add(data1);
            dataRead2.add(data2);

            slot = page.nextSlot(slot);
        }

        Assertions.assertEquals(filteredFields1, dataRead1);
        Assertions.assertEquals(filteredFields2, dataRead2);

        tx.unpin(block);
        tx.commit();
    }

    @AfterAll
    public void shutDown() {
        db.nonStaticFactoryReset();
    }
}
