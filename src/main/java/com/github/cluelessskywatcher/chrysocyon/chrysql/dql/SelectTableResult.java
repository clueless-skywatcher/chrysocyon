package com.github.cluelessskywatcher.chrysocyon.chrysql.dql;

import java.util.List;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

import lombok.Getter;

public class SelectTableResult extends ChrySQLStatementResult {
    private @Getter List<String> fields;
    private @Getter List<List<DataField>> rows;

    public SelectTableResult(List<String> fields, List<List<DataField>> rows, long timeTaken) {
        this.fields = fields;
        this.rows = rows;
        this.timeTaken = timeTaken;
    }
}
