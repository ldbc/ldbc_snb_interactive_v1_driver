package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;

import java.util.Iterator;
import java.util.Map;

public class UpdateOperation extends Operation<Object>
{
    public static final int TYPE = 4;
    private final String table;
    private final String key;
    private final Map<String,Iterator<Byte>> values;

    public UpdateOperation( String table, String key, Map<String,Iterator<Byte>> values )
    {
        this.table = table;
        this.key = key;
        this.values = values;
    }

    public String table()
    {
        return table;
    }

    public String key()
    {
        return key;
    }

    public Map<String,Iterator<Byte>> fields()
    {
        return values;
    }

    @Override
    public Object marshalResult( String serializedOperationResult )
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

        UpdateOperation that = (UpdateOperation) o;

        if ( table != null ? !table.equals( that.table ) : that.table != null )
        { return false; }
        if ( key != null ? !key.equals( that.key ) : that.key != null )
        { return false; }
        return !(values != null ? !values.equals( that.values ) : that.values != null);

    }

    @Override
    public int hashCode()
    {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (values != null ? values.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "UpdateOperation{" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", values=" + values +
               '}';
    }
}
