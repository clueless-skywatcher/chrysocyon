package com.github.cluelessskywatcher.chrysocyon.planning;

import com.github.cluelessskywatcher.chrysocyon.chrysql.dql.SelectFromTableStatement;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public interface QueryPlanner {
    public DatabasePlan createPlan(SelectFromTableStatement stmt, ChrysoTransaction txn);
}
