package com.ldbc.driver.workloads.simple;

import com.ldbc.driver.Operation;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class ScanOperation extends Operation<Vector<Map<String,Iterator<Byte>>>>
{
    public static final int TYPE = 5;

    private final String table;
    private final String startKey;
    private final int recordCount;
    private final List<String> fields;

    public ScanOperation( String table, String startKey, int recordCount, List<String> fields )
    {
        super();
        this.table = table;
        this.startKey = startKey;
        this.recordCount = recordCount;
        this.fields = fields;
    }

    public String table()
    {
        return table;
    }

    public String startkey()
    {
        return startKey;
    }

    public int recordCount()
    {
        return recordCount;
    }

    public List<String> fields()
    {
        return fields;
    }

    @Override
    public Vector<Map<String,Iterator<Byte>>> marshalResult( String serializedOperationResult )
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

        ScanOperation that = (ScanOperation) o;

        if ( recordCount != that.recordCount )
        { return false; }
        if ( table != null ? !table.equals( that.table ) : that.table != null )
        { return false; }
        if ( startKey != null ? !startKey.equals( that.startKey ) : that.startKey != null )
        { return false; }
        return !(fields != null ? !fields.equals( that.fields ) : that.fields != null);

    }

    @Override
    public int hashCode()
    {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (startKey != null ? startKey.hashCode() : 0);
        result = 31 * result + recordCount;
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "ScanOperation{" +
               "table='" + table + '\'' +
               ", startKey='" + startKey + '\'' +
               ", recordCount=" + recordCount +
               ", fields=" + fields +
               '}';
    }
}
