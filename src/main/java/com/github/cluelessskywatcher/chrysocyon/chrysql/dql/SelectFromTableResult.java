package com.github.cluelessskywatcher.chrysocyon.chrysql.dql;

import java.util.List;
import java.util.StringJoiner;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class SelectFromTableResult extends ChrySQLStatementResult {
    private @Getter List<String> fields;
    private @Getter List<List<DataField>> rows;

    public SelectFromTableResult(List<String> fields, List<List<DataField>> rows, long timeTaken) {
        this.fields = fields;
        this.rows = rows;
        this.timeTaken = timeTaken;
    }

    public String toString() {
        StringJoiner endResult = new StringJoiner("\n");
        
        StringJoiner header = new StringJoiner(", ");
        for (String field: fields) {
            header.add(field);
        }

        endResult.add(header.toString());

        for (List<DataField> row: rows) {
            StringJoiner rowStr = new StringJoiner(", ");
            for (DataField data: row) {
                rowStr.add(data.toString());
            }
            endResult.add(rowStr.toString());
        }
        return String.format("%s\n\nTime taken: %d ms", endResult.toString(), timeTaken);
    }
}
