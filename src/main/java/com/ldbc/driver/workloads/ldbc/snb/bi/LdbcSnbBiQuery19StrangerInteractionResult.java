package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery19StrangerInteractionResult
{
    private final long personId;
    private final int strangerCount;
    private final int count;

    public LdbcSnbBiQuery19StrangerInteractionResult( long personId, int strangerCount, int count )
    {
        this.personId = personId;
        this.strangerCount = strangerCount;
        this.count = count;
    }

    public long personId()
    {
        return personId;
    }

    public int strangerCount()
    {
        return strangerCount;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery19Result{" +
               "personId=" + personId +
               ", strangerCount=" + strangerCount +
               ", count=" + count +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery19StrangerInteractionResult that = (LdbcSnbBiQuery19StrangerInteractionResult) o;

        if ( personId != that.personId )
        { return false; }
        if ( strangerCount != that.strangerCount )
        { return false; }
        return count == that.count;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + strangerCount;
        result = 31 * result + count;
        return result;
    }
}
