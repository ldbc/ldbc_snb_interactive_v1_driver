package com.ldbc.workloads2.simple;

import java.util.Map;

import com.ldbc.data.ByteIterator;
import com.ldbc.db2.Operation2;

public class UpdateOperation2 extends Operation2<Object>
{
    private final String table;
    private final String key;
    private final Map<String, ByteIterator> values;

    public UpdateOperation2( String table, String key, Map<String, ByteIterator> values )
    {
        super();
        this.table = table;
        this.key = key;
        this.values = values;
    }

    public String getTable()
    {
        return table;
    }

    public String getKey()
    {
        return key;
    }

    public Map<String, ByteIterator> getValues()
    {
        return values;
    }
}
