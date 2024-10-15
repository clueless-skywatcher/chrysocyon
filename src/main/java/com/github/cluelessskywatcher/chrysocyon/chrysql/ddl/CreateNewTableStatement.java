package com.github.cluelessskywatcher.chrysocyon.chrysql.ddl;

import java.util.StringJoiner;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

import lombok.Getter;

public class CreateNewTableStatement implements ChrySQLStatement {
    private @Getter String tableName;
    private @Getter TupleSchema schema;
    private @Getter ChrySQLStatementResult result;

    public CreateNewTableStatement(String tableName, TupleSchema schema) {
        this.tableName = tableName;
        this.schema = schema;
    }

    @Override
    public void execute(Chrysocyon db, ChrysoTransaction txn) {
        long timeTaken = System.currentTimeMillis();
        MetadataManager mtdm = db.getMetadataManager();
        mtdm.createTable(tableName, schema, txn);
        timeTaken = System.currentTimeMillis() - timeTaken;
        this.result = new CreateNewTableResult(tableName, schema, timeTaken);
    }

    public String toString() {
        StringJoiner joiner = new StringJoiner(", ");
        for (String field : schema.getFields()) {
            joiner.add(String.format("%s %s", schema.getField(field).toString(), field));
        }

        return String.format("create table %s (%s);", tableName, joiner.toString());
    }
}
