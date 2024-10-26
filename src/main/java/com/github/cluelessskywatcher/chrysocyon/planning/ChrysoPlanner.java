package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLParser;
import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.DeleteFromTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.InsertIntoTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dml.UpdateTableStatement;
import com.github.cluelessskywatcher.chrysocyon.chrysql.dql.SelectFromTableStatement;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.Getter;

@SuppressWarnings("all")
public class ChrysoPlanner {
    private @Getter QueryPlanner queryPlanner;
    private @Getter ModifyPlanner modifyPlanner;

    public ChrysoPlanner(QueryPlanner queryPlanner, ModifyPlanner modifyPlanner) {
        this.queryPlanner = queryPlanner;
        this.modifyPlanner = modifyPlanner;
    }

    public DatabasePlan createSelectPlan(String command, ChrysoTransaction txn) {
        ChrySQLParser parser = new ChrySQLParser(command);
        SelectFromTableStatement stmt = (SelectFromTableStatement) parser.parseSelect();
        return queryPlanner.createPlan(stmt, txn);
    }

    public int executeUpdate(String command, ChrysoTransaction txn) {
        ChrySQLParser parser = new ChrySQLParser(command);
        ChrySQLStatement stmt = parser.parseModification();

        if (stmt instanceof InsertIntoTableStatement) {
            return modifyPlanner.executeInsert((InsertIntoTableStatement) stmt, txn);
        } else if (stmt instanceof UpdateTableStatement) {
            return modifyPlanner.executeUpdate((UpdateTableStatement) stmt, txn);
        } else if (stmt instanceof DeleteFromTableStatement) {
            return modifyPlanner.executeDelete((DeleteFromTableStatement) stmt, txn);
        } else {
            return 1;
        }
    }
}
