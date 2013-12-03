package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery7Result
{
    private final String tagName;
    private final long tagCount;

    public LdbcQuery7Result( String tagName, long tagCount )
    {
        super();
        this.tagName = tagName;
        this.tagCount = tagCount;
    }

    public String tagName()
    {
        return tagName;
    }

    public long tagCount()
    {
        return tagCount;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( tagCount ^ ( tagCount >>> 32 ) );
        result = prime * result + ( ( tagName == null ) ? 0 : tagName.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery7Result other = (LdbcQuery7Result) obj;
        if ( tagCount != other.tagCount ) return false;
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
        return "LdbcQuery7Result [tagName=" + tagName + ", tagCount=" + tagCount + "]";
    }
}
