package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.util.Date;
import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery2 extends Operation<List<LdbcQuery2Result>>
{
    public static final int DEFAULT_LIMIT = 20;

    private final long personId;
    private final Date maxDate;
    private final int limit;

    public LdbcQuery2( long personId, Date maxDate, int limit )
    {
        super();
        this.personId = personId;
        this.maxDate = maxDate;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
    }

    public Date maxDate()
    {
        return maxDate;
    }

    public long maxDateAsMilli()
    {
        return maxDate.getTime();
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
        result = prime * result + ( ( maxDate == null ) ? 0 : maxDate.hashCode() );
        result = prime * result + (int) ( personId ^ ( personId >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery2 other = (LdbcQuery2) obj;
        if ( limit != other.limit ) return false;
        if ( maxDate == null )
        {
            if ( other.maxDate != null ) return false;
        }
        else if ( !maxDate.equals( other.maxDate ) ) return false;
        if ( personId != other.personId ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery2 [personId=" + personId + ", maxDate=" + maxDate + ", maxDateAsMilli=" + maxDateAsMilli()
               + ", limit=" + limit + "]";
    }

}
