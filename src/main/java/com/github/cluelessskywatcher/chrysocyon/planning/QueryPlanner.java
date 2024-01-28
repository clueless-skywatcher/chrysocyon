package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.dql.SelectTableStatement;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public interface QueryPlanner {
    public OpPlan createPlan(SelectTableStatement stmt, ChrysoTransaction txn);
}
