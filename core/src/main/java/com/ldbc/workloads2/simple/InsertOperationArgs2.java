package com.ldbc.workloads2.simple;

import java.util.Map;

import com.ldbc.data.ByteIterator;
import com.ldbc.workloads2.OperationArgs2;

public abstract class InsertOperationArgs2 extends OperationArgs2
{
    private final String table;
    private final String keyName;
    private final Map<String, ByteIterator> valuedFields;

    public InsertOperationArgs2( String table, String keyName, Map<String, ByteIterator> valuedFields )
    {
        super();
        this.table = table;
        this.keyName = keyName;
        this.valuedFields = valuedFields;
    }

    public String getTable()
    {
        return table;
    }

    public String getKeyName()
    {
        return keyName;
    }

    public Map<String, ByteIterator> getValuedFields()
    {
        return valuedFields;
    }
}
