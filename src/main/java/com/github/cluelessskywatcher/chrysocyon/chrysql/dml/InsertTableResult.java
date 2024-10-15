package com.github.cluelessskywatcher.chrysocyon.chrysql.dml;

import java.util.ArrayList;
import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.TableConstants;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;

import lombok.Getter;

public class InsertTableResult extends ChrySQLStatementResult {
    private @Getter String tableName;
    private @Getter List<List<DataField>> rows;
    private @Getter List<String> fields;
    
    public InsertTableResult(String tableName, long timeTaken) {
        this.tableName = tableName;
        this.fields = List.of("table_name", "creation_time");
        this.rows = new ArrayList<>();
        this.timeTaken = timeTaken;
        rows.add(
            List.of(
                new VarStringField(tableName, TableConstants.MAX_TABLE_NAME_SIZE),
                new IntegerField((int) timeTaken)
            )
        );
    }

    public String toString() {
        return String.format("INSERT INTO %s. Time taken: %d ms", tableName, timeTaken);
    }
}
