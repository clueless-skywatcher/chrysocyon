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
import com.github.cluelessskywatcher.chrysocyon.processing.scans.TableScan;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

@TestInstance(Lifecycle.PER_CLASS)
public class TableScanTest {
    private Chrysocyon db;
    private ChrysoTransaction transaction;
    private TupleSchema schema;
    private TupleLayout layout;
    private List<Integer> ints;
    private List<String> strs;


    private static final int MAX_VARSTR_LENGTH = 20;

    @BeforeAll
    public void initialize() {
        db = new Chrysocyon("tablescantest", 400, 3);
        transaction = db.newTransaction();
        
        schema = new TupleSchema();
        schema.addField(new IntegerInfo(), "field1");
        schema.addField(new VarStringInfo(MAX_VARSTR_LENGTH), "field2");
        layout = new TupleLayout(schema);
    }

    @Test
    public void testTableScan() {
        TableScan scan = new TableScan(transaction, "table1", layout);
        scan.moveToBeginning();

        ints = new ArrayList<>();
        strs = new ArrayList<>();

        Random random = new Random();

        for (int i = 0; i < 50; i++) {
            scan.insert();
            int n = random.nextInt(100);
            ints.add(n);
            String s = String.format("String%d", n);
            strs.add(s);

            scan.setData("field1", new IntegerField(n));
            scan.setData("field2", new VarStringField(s, MAX_VARSTR_LENGTH));
        }

        scan.moveToBeginning();

        List<Integer> readInts = new ArrayList<>();
        List<String> readStrs = new ArrayList<>();

        while (scan.next()) {
            int a = (int) scan.getData("field1").getValue();
            readInts.add(a);
            String b = (String) scan.getData("field2").getValue();
            readStrs.add(b);
        }

        Assertions.assertEquals(ints, readInts);
        Assertions.assertEquals(strs, readStrs);
    }

    @Test
    public void testDeletedTableScan() {
        TableScan scan = new TableScan(transaction, "table1", layout);
        scan.moveToBeginning();

        List<Integer> filteredInts = ints.stream()
            .filter(entry -> entry >= 25)
            .collect(Collectors.toList());

        List<String> filteredStrs = new ArrayList<>();
        filteredInts.forEach((entry) -> filteredStrs.add(String.format("String%d", entry)));

        while (scan.next()) {
            int a = (int) scan.getData("field1").getValue();
            if (a < 25) {
                scan.delete();
            }
        }

        scan.moveToBeginning();

        List<Integer> readInts = new ArrayList<>();
        List<String> readStrs = new ArrayList<>();

        while (scan.next()) {
            int a = (int) scan.getData("field1").getValue();
            readInts.add(a);
            String b = (String) scan.getData("field2").getValue();
            readStrs.add(b);
        }

        Assertions.assertEquals(filteredInts, readInts);
        Assertions.assertEquals(filteredStrs, readStrs);
    }

    @AfterAll
    public void shutDown() {
        db.nonStaticFactoryReset();
    }
}
