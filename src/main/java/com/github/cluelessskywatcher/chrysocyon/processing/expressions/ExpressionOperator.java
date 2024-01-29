package com.github.cluelessskywatcher.chrysocyon.processing.expressions;

import java.util.HashMap;
import java.util.Map;

public enum ExpressionOperator {
    EQUALS("=");

    private static Map<String, ExpressionOperator> opLookup = new HashMap<>();

    static {
        opLookup.put("=", EQUALS);
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
