package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;
import com.ldbc.driver.data.ByteIterator;

import java.util.List;
import java.util.Map;
import java.util.Vector;

public class ScanOperation extends Operation<Vector<Map<String, ByteIterator>>> {
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
}