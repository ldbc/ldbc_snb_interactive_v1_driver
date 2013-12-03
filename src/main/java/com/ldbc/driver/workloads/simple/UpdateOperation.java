package com.ldbc.driver.workloads.simple;

import java.util.Map;

import com.ldbc.driver.Operation;
import com.ldbc.driver.data.ByteIterator;

public class UpdateOperation extends Operation<Object>
{
    private final String table;
    private final String key;
    private final Map<String, ByteIterator> values;

    public UpdateOperation( String table, String key, Map<String, ByteIterator> values )
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
