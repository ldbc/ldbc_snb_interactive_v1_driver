package com.ldbc.workloads2.simple;

import java.util.Map;
import java.util.Set;

import com.ldbc.data.ByteIterator;
import com.ldbc.db2.Operation2;

public abstract class ReadModifyWriteOperation2 extends Operation2<Object>
{
    private final String table;
    private final String key;
    private final Set<String> fields;
    private final Map<String, ByteIterator> values;

    public ReadModifyWriteOperation2( String table, String key, Set<String> fields, Map<String, ByteIterator> values )
    {
        super();
        this.table = table;
        this.key = key;
        this.fields = fields;
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

    public Set<String> getFields()
    {
        return fields;
    }

    public Map<String, ByteIterator> getValues()
    {
        return values;
    }
}
