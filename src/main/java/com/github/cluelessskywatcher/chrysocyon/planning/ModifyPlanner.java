package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.InsertTableStatement;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public interface ModifyPlanner {
    public int executeCreateTable(CreateTableStatement stmt, ChrysoTransaction txn);
    public int executeInsert(InsertTableStatement stmt, ChrysoTransaction txn);
}
