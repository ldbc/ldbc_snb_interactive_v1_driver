package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery12PersonPostCountsResult
{
    private final int messageCount;
    private final int personCount;

    public LdbcSnbBiQuery12PersonPostCountsResult( int messageCount, int personCount )
    {
        this.messageCount = messageCount;
        this.personCount = personCount;
    }

    public int messageCount()
    {
        return messageCount;
    }

    public int personCount()
    {
        return personCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery12PersonPostCountsResult{" +
               "messageCount=" + messageCount +
               ", personCount=" + personCount +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery12PersonPostCountsResult that = (LdbcSnbBiQuery12PersonPostCountsResult) o;

        if ( messageCount != that.messageCount )
        { return false; }
        return personCount == that.personCount;

    }

    @Override
    public int hashCode()
    {
        int result = messageCount;
        result = 31 * result + personCount;
        return result;
    }
}
