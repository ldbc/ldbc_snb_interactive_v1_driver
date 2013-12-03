package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery3 extends Operation<List<LdbcQuery3Result>>
{
    private final long personId;
    private final String countryX;
    private final String countryY;
    private final Date endDate;
    private final long durationMillis;

    public LdbcQuery3( long personId, String countryX, String countryY, Date endDate, long durationMillis )
    {
        super();
        this.personId = personId;
        this.countryX = countryX;
        this.countryY = countryY;
        this.endDate = endDate;
        this.durationMillis = durationMillis;
    }

    public long personId()
    {
        return personId;
    }

    public String countryX()
    {
        return countryX;
    }

    public String countryY()
    {
        return countryY;
    }

    public Date endDate()
    {
        return endDate;
    }

    public long durationMillis()
    {
        return durationMillis;
    }

    public long startDateAsMilli()
    {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTime( endDate );
        return c.getTimeInMillis() - durationMillis;
    }

    public long endDateAsMilli()
    {
        return endDate.getTime();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( countryX == null ) ? 0 : countryX.hashCode() );
        result = prime * result + ( ( countryY == null ) ? 0 : countryY.hashCode() );
        result = prime * result + (int) ( durationMillis ^ ( durationMillis >>> 32 ) );
        result = prime * result + ( ( endDate == null ) ? 0 : endDate.hashCode() );
        result = prime * result + (int) ( personId ^ ( personId >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery3 other = (LdbcQuery3) obj;
        if ( countryX == null )
        {
            if ( other.countryX != null ) return false;
        }
        else if ( !countryX.equals( other.countryX ) ) return false;
        if ( countryY == null )
        {
            if ( other.countryY != null ) return false;
        }
        else if ( !countryY.equals( other.countryY ) ) return false;
        if ( durationMillis != other.durationMillis ) return false;
        if ( endDate == null )
        {
            if ( other.endDate != null ) return false;
        }
        else if ( !endDate.equals( other.endDate ) ) return false;
        if ( personId != other.personId ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery3 [personId=" + personId + ", countryX=" + countryX + ", countryY=" + countryY + ", endDate="
               + endDate + ", durationMillis=" + durationMillis + "' startDateAsMilli=" + startDateAsMilli()
               + ", endDateAsMilli=" + endDateAsMilli() + "]";
    }
}
