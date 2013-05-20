package com.ldbc.workloads.simple;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.ldbc.Operation;
import com.ldbc.data.ByteIterator;

public class ScanOperation extends Operation<Vector<Map<String, ByteIterator>>>
{
    private final String table;
    private final String startKey;
    private final int recordCount;
    private final Set<String> fields;

    public ScanOperation( String table, String startKey, int recordCount, Set<String> fields )
    {
        super();
        this.table = table;
        this.startKey = startKey;
        this.recordCount = recordCount;
        this.fields = fields;
    }

    public String getTable()
    {
        return table;
    }

    public String getStartkey()
    {
        return startKey;
    }

    public int getRecordcount()
    {
        return recordCount;
    }

    public Set<String> getFields()
    {
        return fields;
    }
}
