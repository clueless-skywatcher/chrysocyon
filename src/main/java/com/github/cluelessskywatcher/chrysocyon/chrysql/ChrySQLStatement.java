package com.github.cluelessskywatcher.chrysocyon.chrysql;

import com.github.cluelessskywatcher.chrysocyon.metadata.MetadataManager;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public interface ChrySQLStatement {
    public void execute(MetadataManager mtdm, ChrysoTransaction txn);
}
