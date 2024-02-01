package com.github.cluelessskywatcher.chrysocyon.chrysql.dml;

import java.util.List;
import java.util.StringJoiner;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.planning.BasicModifyPlanner;
import com.github.cluelessskywatcher.chrysocyon.planning.ModifyPlanner;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class InsertTableStatement implements ChrySQLStatement {
    private @Getter String tableName;
    private @Getter List<String> fieldNames;
    private @Getter List<DataField> values;
    private @Getter ChrySQLStatementResult result;

    public InsertTableStatement(String tableName, List<String> fieldNames, List<DataField> values) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.values = values;
    }

    public String toString() {
        StringJoiner fields = new StringJoiner(", ");
        for (String field : fieldNames) {
            fields.add(field);
        }

        StringJoiner valuesString = new StringJoiner(", ");
        for (DataField table : values) {
            valuesString.add(table.toString());
        }

        return String.format(
            "insert into %s (%s) values (%s);",
            tableName,
            fields.toString(),
            valuesString.toString()
        );
    }

    @Override
    public void execute(Chrysocyon db, ChrysoTransaction txn) {
        MetadataManager mtdm = db.getMetadataManager();
        ModifyPlanner planner = new BasicModifyPlanner(mtdm);
        long timeTaken = System.currentTimeMillis();
        int result = planner.executeInsert(this, txn);
        timeTaken = System.currentTimeMillis() - timeTaken;
        if (result == 1) {
            this.result = new InsertTableResult(tableName, timeTaken);        
        }
    }
}
