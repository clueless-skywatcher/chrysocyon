package com.github.cluelessskywatcher.chrysocyon.tuples.info;

import java.util.HashMap;
import java.util.Map;

public enum TupleDataType {
    INTEGER(0),
    VARSTR(1);

    private int code;
    private static Map<Integer, TupleDataType> code2Type = new HashMap<>();

    static {
        for (TupleDataType value: values()) {
            code2Type.put(value.code, value);
        }
    }

    private TupleDataType(int code) {
        this.code = code;
    }

    public static TupleDataType getTypeFromCode(int code) {
        return code2Type.get(code);
    }

    public int getCode() {
        return this.code;
    }
}
