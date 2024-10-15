package com.github.cluelessskywatcher.chrysocyon.processing.expressions;

import java.util.HashMap;
import java.util.Map;

public enum ExpressionOperator {
    EQUALS("="),
    GT(">"),
    LT("<"),
    NOT_EQUALS("!=");

    private static Map<String, ExpressionOperator> opLookup = new HashMap<>();

    static {
        opLookup.put("=", EQUALS);
        opLookup.put(">", GT);
        opLookup.put("<", LT);
        opLookup.put("!=", NOT_EQUALS);
    }

    private String op;

    private ExpressionOperator(String op) {
        this.op = op;
    }

    public static ExpressionOperator getOpFromString(String str) {
        return opLookup.get(str);
    }

    public String toString() {
        return this.op;
    }
}
