package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;
import com.ldbc.driver.data.ByteIterator;

import java.util.List;
import java.util.Map;

public class ReadOperation extends Operation<Map<String, ByteIterator>> {
    private final String table;
    private final String key;
    private final List<String> fields;

    public ReadOperation(String table, String key, List<String> fields) {
        this.table = table;
        this.key = key;
        this.fields = fields;
    }

    public String getTable() {
        return table;
    }

    public String getKey() {
        return key;
    }

    public List<String> getFields() {
        return fields;
    }

    @Override
    public Map<String, ByteIterator> marshalResult(String serializedOperationResult) {
        return null;
    }

    @Override
    public String serializeResult(Object operationResultInstance) {
        return null;
    }
}
