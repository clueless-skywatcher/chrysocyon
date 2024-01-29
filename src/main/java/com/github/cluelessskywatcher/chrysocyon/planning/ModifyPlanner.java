package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.ChrySQLStatement;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public interface ModifyPlanner {
    public OpPlan createModificationPlan(ChrySQLStatement stmt, ChrysoTransaction txn);
}
