package com.github.cluelessskywatcher.chrysocyon.tuples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.cluelessskywatcher.chrysocyon.tuples.info.DataInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.TupleDataType;

public class TupleSchema {
    private List<String> fields = new ArrayList<>();
    private Map<String, DataInfo> info = new HashMap<>();

    public TupleSchema(){
    }

    public void addField(DataInfo field, String name) {
        fields.add(name);
        info.put(name, field);   
    }

    public void addFromSchema(String field, TupleSchema schema) {
        DataInfo dInfo = schema.getField(field);
        addField(dInfo, field);
    }

    public void addAll(TupleSchema schema) {
        for (String fieldName : schema.getFields()) {
            addFromSchema(fieldName, schema);
        }
    }

    public List<String> getFields() {
        return fields;
    }

    public boolean hasField(String fieldName) {
        return info.containsKey(fieldName);
    }

    public TupleDataType getType(String fieldName) {
        return info.get(fieldName).getDataType();
    }

    public DataInfo getField(String fieldName) {
        return info.get(fieldName);
    }

    public int length(String fieldName) {
        return info.get(fieldName).getSize();
    }
}
