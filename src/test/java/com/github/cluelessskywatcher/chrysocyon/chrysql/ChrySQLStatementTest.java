package com.github.cluelessskywatcher.chrysocyon.chrysql;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dql.SelectFromTableResult;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;

@TestInstance(Lifecycle.PER_CLASS)
public class ChrySQLStatementTest {
    private Chrysocyon db;
    private ChrysoTransaction transaction;
    
    @BeforeAll
    public void init() {
        db = new Chrysocyon("stmttest");
        transaction = db.newTransaction();
        
        ChrySQLUtils.execute(
            "create table human_species (int species_number, varstr(100) scientific_name);", db, transaction
        );
        
        ChrySQLUtils.execute(
            "insert into human_species(species_number, scientific_name) values (1, \"Homo habilis\");",
            db, transaction
        );
        ChrySQLUtils.execute(
            "insert into human_species(species_number, scientific_name) values (2, \"Homo erectus\");",
            db, transaction
        );
        ChrySQLUtils.execute(
            "insert into human_species(species_number, scientific_name) values (3, \"Homo neanderthalensis\");",
            db, transaction
        );
        ChrySQLUtils.execute(
            "insert into human_species(species_number, scientific_name) values (4, \"Homo sapiens\");",
            db, transaction
        );
    }

    @Test
    public void testPlainSelect() {
        SelectFromTableResult selTable1 = (SelectFromTableResult) ChrySQLUtils.execute(
            "select * from human_species;", 
            db, transaction
        );
        Assertions.assertEquals(selTable1.getFields(), List.of("species_number", "scientific_name"));
        Assertions.assertEquals(selTable1.getRows(), 
            List.of(
                List.of(new IntegerField(1), new VarStringField("Homo habilis", 100)),
                List.of(new IntegerField(2), new VarStringField("Homo erectus", 100)),
                List.of(new IntegerField(3), new VarStringField("Homo neanderthalensis", 100)),
                List.of(new IntegerField(4), new VarStringField("Homo sapiens", 100))
            )
        );
    }

    @Test
    public void testWithFilter() {
        SelectFromTableResult selTable2 = (SelectFromTableResult) ChrySQLUtils.execute(
            "select * from human_species where species_number = 1;", 
            db, transaction
        );
        Assertions.assertEquals(selTable2.getRows(), 
            List.of(
                List.of(new IntegerField(1), new VarStringField("Homo habilis", 100))
            )
        );
    }

    @AfterAll
    public void shutDown() {
        db.nonStaticFactoryReset();
    }
}
