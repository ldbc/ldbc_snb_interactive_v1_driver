package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class ScanOperation extends Operation<Vector<Map<String, Iterator<Byte>>>> {
    private final String table;
    private final String startKey;
    private final int recordCount;
    private final List<String> fields;

    public ScanOperation(String table, String startKey, int recordCount, List<String> fields) {
        super();
        this.table = table;
        this.startKey = startKey;
        this.recordCount = recordCount;
        this.fields = fields;
    }

    public String getTable() {
        return table;
    }

    public String getStartkey() {
        return startKey;
    }

    public int getRecordcount() {
        return recordCount;
    }

    public List<String> getFields() {
        return fields;
    }

    @Override
    public Vector<Map<String, Iterator<Byte>>> marshalResult(String serializedOperationResult) {
        return null;
    }

    @Override
    public String serializeResult(Object operationResultInstance) {
        return null;
    }
}
