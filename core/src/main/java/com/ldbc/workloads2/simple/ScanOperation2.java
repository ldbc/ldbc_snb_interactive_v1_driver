package com.ldbc.workloads2.simple;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.ldbc.data.ByteIterator;
import com.ldbc.db2.Operation2;

public abstract class ScanOperation2 extends Operation2<Vector<Map<String, ByteIterator>>>
{
    private final String table;
    private final String startKey;
    private final int recordCount;
    private final Set<String> fields;

    public ScanOperation2( String table, String startKey, int recordCount, Set<String> fields )
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
