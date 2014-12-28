package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;

import java.util.Iterator;
import java.util.Map;

public class InsertOperation extends Operation<Object> {
    private final String table;
    private final String key;
    private final Map<String, Iterator<Byte>> valuedFields;

    public InsertOperation(String table, String key, Map<String, Iterator<Byte>> valuedFields) {
        super();
        this.table = table;
        this.key = key;
        this.valuedFields = valuedFields;
    }

    public String getTable() {
        return table;
    }

    public String getKey() {
        return key;
    }

    public Map<String, Iterator<Byte>> getValuedFields() {
        return valuedFields;
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
        return 2;
    }
}
