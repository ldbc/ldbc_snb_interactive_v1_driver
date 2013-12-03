package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery4Result
{
    private final String tagName;
    private final int tagCount;

    public LdbcQuery4Result( String tagName, int tagCount )
    {
        super();
        this.tagName = tagName;
        this.tagCount = tagCount;
    }

    public String tagName()
    {
        return tagName;
    }

    public int tagCount()
    {
        return tagCount;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + tagCount;
        result = prime * result + ( ( tagName == null ) ? 0 : tagName.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery4Result other = (LdbcQuery4Result) obj;
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
        return "LdbcQuery4Result [tagName=" + tagName + ", tagCount=" + tagCount + "]";
    }
}
