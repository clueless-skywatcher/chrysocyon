package com.github.cluelessskywatcher.chrysocyon.chrysql;

import com.github.cluelessskywatcher.chrysocyon.Chrysocyon;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;

public class ChrySQLUtils {
    public static ChrySQLStatementResult execute(String query, Chrysocyon db, ChrysoTransaction tx) {
        ChrySQLParser parser = new ChrySQLParser(query);
        ChrySQLStatement stmt = parser.parse();
        stmt.execute(db, tx);
        return stmt.getResult();
    }
}
