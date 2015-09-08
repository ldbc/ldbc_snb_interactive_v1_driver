package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ReadModifyWriteOperation extends Operation<Object>
{
    public static final int TYPE = 1;

    private final String table;
    private final String key;
    private final List<String> readFields;
    private final Map<String,Iterator<Byte>> writeValues;

    public ReadModifyWriteOperation( String table, String key, List<String> readFields,
            Map<String,Iterator<Byte>> writeValues )
    {
        super();
        this.table = table;
        this.key = key;
        this.readFields = readFields;
        this.writeValues = writeValues;
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
        return readFields;
    }

    public Map<String,Iterator<Byte>> values()
    {
        return writeValues;
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

        ReadModifyWriteOperation that = (ReadModifyWriteOperation) o;

        if ( table != null ? !table.equals( that.table ) : that.table != null )
        { return false; }
        if ( key != null ? !key.equals( that.key ) : that.key != null )
        { return false; }
        if ( readFields != null ? !readFields.equals( that.readFields ) : that.readFields != null )
        { return false; }
        return !(writeValues != null ? !writeValues.equals( that.writeValues ) : that.writeValues != null);

    }

    @Override
    public int hashCode()
    {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (readFields != null ? readFields.hashCode() : 0);
        result = 31 * result + (writeValues != null ? writeValues.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "ReadModifyWriteOperation{" +
               "table='" + table + '\'' +
               ", key='" + key + '\'' +
               ", readFields=" + readFields +
               ", writeValues=" + writeValues +
               '}';
    }
}
