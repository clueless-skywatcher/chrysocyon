package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.InsertTableStatement;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;

public interface ModifyPlanner {
    public int executeCreateTable(CreateTableStatement stmt, ChrysoTransaction txn);
    public int executeInsert(InsertTableStatement stmt, ChrysoTransaction txn);
}
