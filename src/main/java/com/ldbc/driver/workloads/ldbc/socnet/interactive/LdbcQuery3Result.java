package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery3Result
{
    private final String friendName;
    private final long xCount;
    private final long yCount;

    public LdbcQuery3Result( String friendName, long xCount, long yCount )
    {
        super();
        this.friendName = friendName;
        this.xCount = xCount;
        this.yCount = yCount;
    }

    public String friendName()
    {
        return friendName;
    }

    public long xCount()
    {
        return xCount;
    }

    public long yCount()
    {
        return yCount;
    }

    public long xyCount()
    {
        return xCount + yCount;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( friendName == null ) ? 0 : friendName.hashCode() );
        result = prime * result + (int) ( xCount ^ ( xCount >>> 32 ) );
        result = prime * result + (int) ( yCount ^ ( yCount >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery3Result other = (LdbcQuery3Result) obj;
        if ( friendName == null )
        {
            if ( other.friendName != null ) return false;
        }
        else if ( !friendName.equals( other.friendName ) ) return false;
        if ( xCount != other.xCount ) return false;
        if ( yCount != other.yCount ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery3Result [friendName=" + friendName + ", xCount=" + xCount + ", yCount=" + yCount + "]";
    }
}
