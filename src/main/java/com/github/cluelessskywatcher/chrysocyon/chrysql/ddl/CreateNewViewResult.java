package com.github.cluelessskywatcher.chrysocyon.chrysql.ddl;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;

import lombok.Getter;

public class CreateNewViewResult extends ChrySQLStatementResult {
    private @Getter String viewName;
    private @Getter ChrySQLStatement query;

    public CreateNewViewResult(String viewName, ChrySQLStatement query, long timeTaken) {
        this.viewName = viewName;
        this.query = query;
        this.timeTaken = timeTaken;
    }

    public String toString() {
        return String.format("CREATE VIEW. Time taken: %s", timeTaken);
    }
}
