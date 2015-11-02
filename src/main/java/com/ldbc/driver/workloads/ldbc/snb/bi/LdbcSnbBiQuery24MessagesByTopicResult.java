package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery24MessagesByTopicResult
{
    public static final int DEFAULT_LIMIT = 100;
    private final int messageCount;
    private final int likeCount;
    private final int year;
    private final int month;
    private final String continent;

    public LdbcSnbBiQuery24MessagesByTopicResult(
            int messageCount,
            int likeCount,
            int year,
            int month,
            String continent )
    {
        this.messageCount = messageCount;
        this.likeCount = likeCount;
        this.year = year;
        this.month = month;
        this.continent = continent;
    }

    public int messageCount()
    {
        return messageCount;
    }

    public int likeCount()
    {
        return likeCount;
    }

    public int year()
    {
        return year;
    }

    public int month()
    {
        return month;
    }

    public String continent()
    {
        return continent;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery24MessagesByTopicResult{" +
               "messageCount=" + messageCount +
               ", likeCount=" + likeCount +
               ", year=" + year +
               ", month=" + month +
               ", continent='" + continent + '\'' +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery24MessagesByTopicResult that = (LdbcSnbBiQuery24MessagesByTopicResult) o;

        if ( messageCount != that.messageCount )
        { return false; }
        if ( likeCount != that.likeCount )
        { return false; }
        if ( year != that.year )
        { return false; }
        if ( month != that.month )
        { return false; }
        return !(continent != null ? !continent.equals( that.continent ) : that.continent != null);

    }

    @Override
    public int hashCode()
    {
        int result = messageCount;
        result = 31 * result + likeCount;
        result = 31 * result + year;
        result = 31 * result + month;
        result = 31 * result + (continent != null ? continent.hashCode() : 0);
        return result;
    }
}
