package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ReadOperation extends Operation<Map<String,Iterator<Byte>>>
{
    public static final int TYPE = 3;

    private final String table;
    private final String key;
    private final List<String> fields;

    public ReadOperation( String table, String key, List<String> fields )
    {
        this.table = table;
        this.key = key;
        this.fields = fields;
    }

    public String table()
    {
        return table;
    }

    public String key()
    {
        return key;
    }

    public List<String> fields()
    {
        return fields;
    }

    @Override
    public Map<String,Iterator<Byte>> marshalResult( String serializedOperationResult )
    {
        return null;
    }

    @Override
    public String serializeResult( Object operationResultInstance )
    {
        return null;
    }

    @Override
    public int type()
    {
        return TYPE;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        ReadOperation that = (ReadOperation) o;

        if ( table != null ? !table.equals( that.table ) : that.table != null )
        { return false; }
        if ( key != null ? !key.equals( that.key ) : that.key != null )
        { return false; }
        return !(fields != null ? !fields.equals( that.fields ) : that.fields != null);

    }

    @Override
    public int hashCode()
    {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "ReadOperation{" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", fields=" + fields +
               '}';
    }
}
