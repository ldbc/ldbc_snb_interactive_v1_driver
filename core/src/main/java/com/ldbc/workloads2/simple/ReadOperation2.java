package com.ldbc.workloads2.simple;

import java.util.Map;
import java.util.Set;

import com.ldbc.data.ByteIterator;
import com.ldbc.db2.Operation2;

public class ReadOperation2 extends Operation2<Map<String, ByteIterator>>
{
    private final String table;
    private final String key;
    private final Set<String> fields;

    public ReadOperation2( String table, String key, Set<String> fields )
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
