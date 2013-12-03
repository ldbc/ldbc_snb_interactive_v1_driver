package com.ldbc.driver.workloads.simple;

import java.util.Map;

import com.ldbc.driver.Operation;
import com.ldbc.driver.data.ByteIterator;

public class InsertOperation extends Operation<Object>
{
    private final String table;
    private final String key;
    private final Map<String, ByteIterator> valuedFields;

    public InsertOperation( String table, String key, Map<String, ByteIterator> valuedFields )
    {
        super();
        this.table = table;
        this.key = key;
        this.valuedFields = valuedFields;
    }

    public String getTable()
    {
        return table;
    }

    public String getKey()
    {
        return key;
    }

    public Map<String, ByteIterator> getValuedFields()
    {
        return valuedFields;
    }
}
