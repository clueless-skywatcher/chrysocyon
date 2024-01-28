package com.github.cluelessskywatcher.chrysocyon.tuples.info;

import java.util.HashMap;
import java.util.Map;

public enum TupleDataType {
    INTEGER(0, "int"),
    VARSTR(1, "varstr");

    private int code;
    private String repr;
    private static Map<Integer, TupleDataType> code2Type = new HashMap<>();

    static {
        for (TupleDataType value: values()) {
            code2Type.put(value.code, value);
        }
    }

    private TupleDataType(int code, String repr) {
        this.code = code;
        this.repr = repr;
    }

    public static TupleDataType getTypeFromCode(int code) {
        return code2Type.get(code);
    }

    public int getCode() {
        return this.code;
    }

    public String toString() {
        return repr;
    }
}
