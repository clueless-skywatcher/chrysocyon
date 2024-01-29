package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLParser;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

import lombok.Getter;

public class ChrySQLPlanner {
    private @Getter QueryPlanner queryPlanner;
    private @Getter ModifyPlanner modifyPlanner;

    public ChrySQLPlanner(QueryPlanner queryPlanner, ModifyPlanner modifyPlanner) {
        this.queryPlanner = queryPlanner;
        this.modifyPlanner = modifyPlanner;
    }
}
