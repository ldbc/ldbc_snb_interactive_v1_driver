package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery19StrangerInteractionResult
{
    private final long personId;
    private final int strangerCount;
    private final int interactionCount;

    public LdbcSnbBiQuery19StrangerInteractionResult( long personId, int strangerCount, int interactionCount)
    {
        this.personId = personId;
        this.strangerCount = strangerCount;
        this.interactionCount = interactionCount;
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
        return interactionCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery19StrangerInteractionResult{" +
               "personId=" + personId +
               ", strangerCount=" + strangerCount +
               ", interactionCount=" + interactionCount +
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
        return interactionCount == that.interactionCount;

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + strangerCount;
        result = 31 * result + interactionCount;
        return result;
    }
}
