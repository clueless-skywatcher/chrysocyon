package com.github.cluelessskywatcher.chrysocyon.chrysql.ddl;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatementResult;
import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.Getter;

public class CreateNewViewStatement implements ChrySQLStatement {
    private @Getter String viewName;
    private @Getter ChrySQLStatement query;
    private @Getter ChrySQLStatementResult result;

    public CreateNewViewStatement(String viewName, ChrySQLStatement query) {
        this.viewName = viewName;
        this.query = query;
    }

    @Override
    public void execute(Chrysocyon db, ChrysoTransaction tx) {
        long timeTaken = System.currentTimeMillis();
        MetadataManager mtdm = db.getMetadataManager();
        mtdm.createView(viewName, query.toString(), tx);
        timeTaken = System.currentTimeMillis() - timeTaken;
        this.result = new CreateNewViewResult(viewName, query, timeTaken);
    }

    public String toString() {
        return String.format("create view %s as %s", viewName, query.toString());
    }
}
