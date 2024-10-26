package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateNewIndexStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateNewTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ddl.CreateNewViewStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.DeleteFromTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.InsertIntoTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.UpdateTableStatement;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public interface ModifyPlanner {
    public int executeCreateTable(CreateNewTableStatement stmt, ChrysoTransaction txn);
    public int executeCreateIndex(CreateNewIndexStatement stmt, ChrysoTransaction txn);
    public int executeCreateView(CreateNewViewStatement stmt, ChrysoTransaction txn);
    public int executeInsert(InsertIntoTableStatement stmt, ChrysoTransaction txn);
    public int executeUpdate(UpdateTableStatement stmt, ChrysoTransaction txn);
    public int executeDelete(DeleteFromTableStatement stmt, ChrysoTransaction txn);
}
