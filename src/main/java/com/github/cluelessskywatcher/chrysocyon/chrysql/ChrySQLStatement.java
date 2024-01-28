package com.github.cluelessskywatcher.chrysocyon.chrysql;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public interface ChrySQLStatement {
    public void execute(Chrysocyon db, ChrysoTransaction tx);
    public ChrySQLStatementResult getResult();
}
