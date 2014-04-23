package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery6 extends Operation<List<LdbcQuery6Result>>
{
    public static final int DEFAULT_LIMIT = 10;

    private final long personId;
    private final String tagName;
    private final int limit;

    public LdbcQuery6( long personId, String tagName, int limit )
    {
        super();
        this.personId = personId;
        this.tagName = tagName;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
    }

    public String tagName()
    {
        return tagName;
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + limit;
        result = prime * result + (int) ( personId ^ ( personId >>> 32 ) );
        result = prime * result + ( ( tagName == null ) ? 0 : tagName.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery6 other = (LdbcQuery6) obj;
        if ( limit != other.limit ) return false;
        if ( personId != other.personId ) return false;
        if ( tagName == null )
        {
            if ( other.tagName != null ) return false;
        }
        else if ( !tagName.equals( other.tagName ) ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery6 [personId=" + personId + ", tagName=" + tagName + ", limit=" + limit + "]";
    }
}
