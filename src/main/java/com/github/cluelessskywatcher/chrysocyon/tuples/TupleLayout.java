package com.github.cluelessskywatcher.chrysocyon.tuples;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

public class TupleLayout {
    private @Getter TupleSchema schema;
    private @Getter Map<String, Integer> offsets;
    private @Getter int slotSize;

    public TupleLayout(TupleSchema schema) {
        this.schema = schema;
        offsets = new HashMap<>();
        // To store the dirty flag (flag to signify that this slot is being used)
        int pos = Integer.BYTES; 
        for (String field : schema.getFields()) {
            offsets.put(field, pos);
            pos += schema.getField(field).getSize();
        }
        slotSize = pos;
    }

    public TupleLayout(TupleSchema schema, Map<String, Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    public int getOffset(String fieldName) {
        return offsets.get(fieldName);
    }
}
