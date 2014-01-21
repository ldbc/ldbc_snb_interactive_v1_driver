package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;
import com.ldbc.driver.data.ByteIterator;

import java.util.List;
import java.util.Map;

public class ReadModifyWriteOperation extends Operation<Object> {
    private final String table;
    private final String key;
    private final List<String> readFields;
    private final Map<String, ByteIterator> writeValues;

    public ReadModifyWriteOperation(String table, String key, List<String> readFields,
                                    Map<String, ByteIterator> writeValues) {
        super();
        this.table = table;
        this.key = key;
        this.readFields = readFields;
        this.writeValues = writeValues;
    }

    public String getTable() {
        return table;
    }

    public String getKey() {
        return key;
    }

    public List<String> getFields() {
        return readFields;
    }

    public Map<String, ByteIterator> getValues() {
        return writeValues;
    }
}
