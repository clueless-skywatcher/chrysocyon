package com.github.cluelessskywatcher.chrysocyon.chrysql.ddl;

import java.util.ArrayList;
import java.util.List;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.metadata.metatables.constants.TableConstants;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.IntegerField;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.VarStringField;

import lombok.Getter;

public class CreateTableResult extends ChrySQLStatementResult {
    private @Getter String tableName;
    private @Getter List<List<DataField>> rows;
    private @Getter List<String> fields;

    public CreateTableResult(String tableName, TupleSchema schema, long timeTaken) {
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
        return String.format("CREATE TABLE %s. Time taken: %d ms", tableName, timeTaken);
    }
}
