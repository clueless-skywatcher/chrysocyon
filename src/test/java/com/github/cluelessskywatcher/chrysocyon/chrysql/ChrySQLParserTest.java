package com.github.cluelessskywatcher.chrysocyon.chrysql;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.InsertTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dql.SelectTableStatement;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.PredExpression;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.PredTerm;
import com.github.cluelessskywatcher.chrysocyon.processing.expressions.QueryPredicate;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.TupleDataType;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

@TestInstance(Lifecycle.PER_CLASS)
public class ChrySQLParserTest {
    @Test
    public void testSelect() {
        String q1 = "select field1, field2 from table1;";
        ChrySQLParser parser = new ChrySQLParser(q1);
        SelectTableStatement stmt = (SelectTableStatement) parser.parseSelect();

        Assertions.assertEquals(List.of("table1"), stmt.getTableNames());
        Assertions.assertEquals(List.of("field1", "field2"), stmt.getSelectFields());
        Assertions.assertEquals(new QueryPredicate(), stmt.getPredicate());

        String q2 = "select field1, field2 from table1, table2 where field1 = 25;";
        parser = new ChrySQLParser(q2);
        stmt = (SelectTableStatement) parser.parseSelect();

        Assertions.assertEquals(List.of("table1", "table2"), stmt.getTableNames());
        Assertions.assertEquals(List.of("field1", "field2"), stmt.getSelectFields());
        Assertions.assertEquals(new QueryPredicate(
            new PredTerm(new PredExpression("field1"), new PredExpression(new IntegerField(25)))
        ), stmt.getPredicate());
        Assertions.assertEquals(q2, stmt.toString());

        String q3 = "select * from table1, table2 where field1 = 25;";
        parser = new ChrySQLParser(q3);
        stmt = (SelectTableStatement) parser.parseSelect();

        Assertions.assertEquals(List.of("table1", "table2"), stmt.getTableNames());
        Assertions.assertEquals(List.of(), stmt.getSelectFields());
        Assertions.assertEquals(new QueryPredicate(
            new PredTerm(new PredExpression("field1"), new PredExpression(new IntegerField(25)))
        ), stmt.getPredicate());
        Assertions.assertEquals(stmt.toString(), q3);
    }

    @Test
    public void testInsert() {
        String q1 = "insert into table1 (field1, field2) values (1, 2);";
        ChrySQLParser parser = new ChrySQLParser(q1);
        InsertTableStatement stmt = (InsertTableStatement) parser.parseModification();

        Assertions.assertEquals("table1", stmt.getTableName());
        Assertions.assertEquals(List.of("field1", "field2"), stmt.getFieldNames());
        Assertions.assertEquals(List.of(new IntegerField(1), new IntegerField(2)), stmt.getValues());

        Assertions.assertEquals(stmt.toString(), q1);
    }

    @Test
    public void testAnyQuery() {
        String q1 = "select * from table1;";
        ChrySQLParser parser = new ChrySQLParser(q1);
        SelectTableStatement stmt1 = (SelectTableStatement) parser.parse();
        Assertions.assertEquals(List.of("table1"), stmt1.getTableNames());
        Assertions.assertEquals(List.of(), stmt1.getSelectFields());
        Assertions.assertEquals(new QueryPredicate(), stmt1.getPredicate());

        String q2 = "insert into table1 (field1, field2) values (1, 2);";
        parser = new ChrySQLParser(q2);
        InsertTableStatement stmt2 = (InsertTableStatement) parser.parse();
        Assertions.assertEquals("table1", stmt2.getTableName());
        Assertions.assertEquals(List.of("field1", "field2"), stmt2.getFieldNames());
        Assertions.assertEquals(List.of(new IntegerField(1), new IntegerField(2)), stmt2.getValues());
    
        String q3 = "create table table1 (int field1, varstr(25) field2);";
        parser = new ChrySQLParser(q3);
        CreateTableStatement stmt3 = (CreateTableStatement) parser.parse();
        Assertions.assertEquals("table1", stmt3.getTableName());
        Assertions.assertEquals(List.of("field1", "field2"), stmt3.getSchema().getFields());
        Assertions.assertEquals(TupleDataType.INTEGER, stmt3.getSchema().getType("field1"));
        Assertions.assertEquals(TupleDataType.VARSTR, stmt3.getSchema().getType("field2"));
        Assertions.assertEquals(25, ((VarStringInfo) stmt3.getSchema().getField("field2")).getCharSize());
        Assertions.assertEquals(q3, stmt3.toString());
    }
}
