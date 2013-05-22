package com.ldbc.driver.workloads.simple;

import java.util.Map;
import java.util.Set;

import com.ldbc.driver.Operation;
import com.ldbc.driver.data.ByteIterator;

public class ReadOperation extends Operation<Map<String, ByteIterator>>
{
    private final String table;
    private final String key;
    private final Set<String> fields;

    public ReadOperation( String table, String key, Set<String> fields )
    {
        super();
        this.table = table;
        this.key = key;
        this.fields = fields;
    }

    public String getTable()
    {
        return table;
    }

    public String getKey()
    {
        return key;
    }

    public Set<String> getFields()
    {
        return fields;
    }
}
