package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;

import java.util.Iterator;
import java.util.Map;

public class UpdateOperation extends Operation<Object> {
    private final String table;
    private final String key;
    private final Map<String, Iterator<Byte>> values;

    public UpdateOperation(String table, String key, Map<String, Iterator<Byte>> values) {
        this.table = table;
        this.key = key;
        this.values = values;
    }

    public String getTable() {
        return table;
    }

    public String getKey() {
        return key;
    }

    public Map<String, Iterator<Byte>> getValues() {
        return values;
    }

    @Override
    public Object marshalResult(String serializedOperationResult) {
        return null;
    }

    @Override
    public String serializeResult(Object operationResultInstance) {
        return null;
    }

    @Override
    public int type() {
        return 4;
    }
}
