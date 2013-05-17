package com.ldbc.workloads2.simple;

import java.util.Map;
import java.util.Set;

import com.ldbc.data.ByteIterator;
import com.ldbc.db2.Operation2;

public class ReadModifyWriteOperation2 extends Operation2<Object>
{
    private final String table;
    private final String key;
    private final Set<String> readFields;
    private final Map<String, ByteIterator> writeValues;

    public ReadModifyWriteOperation2( String table, String key, Set<String> readFields,
            Map<String, ByteIterator> writeValues )
    {
        super();
        this.table = table;
        this.key = key;
        this.readFields = readFields;
        this.writeValues = writeValues;
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
        return readFields;
    }

    public Map<String, ByteIterator> getValues()
    {
        return writeValues;
    }
}
