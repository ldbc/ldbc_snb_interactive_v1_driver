package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery1 extends Operation<List<LdbcQuery1Result>>
{
    private final String firstName;
    private final int limit;

    public LdbcQuery1( String firstName, int limit )
    {
        super();
        this.firstName = firstName;
        this.limit = limit;
    }

    public String firstName()
    {
        return firstName;
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
        result = prime * result + ( ( firstName == null ) ? 0 : firstName.hashCode() );
        result = prime * result + limit;
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery1 other = (LdbcQuery1) obj;
        if ( firstName == null )
        {
            if ( other.firstName != null ) return false;
        }
        else if ( !firstName.equals( other.firstName ) ) return false;
        if ( limit != other.limit ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery1 [firstName=" + firstName + ", limit=" + limit + "]";
    }
}
