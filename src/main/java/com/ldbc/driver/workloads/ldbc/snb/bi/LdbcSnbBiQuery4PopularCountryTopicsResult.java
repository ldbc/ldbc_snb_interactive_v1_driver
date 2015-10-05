package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery4PopularCountryTopicsResult
{
    private final long forumId;
    private final String forumTitle;
    private final long forumCreationDate;
    private final long moderatorId;
    private final int count;

    public LdbcSnbBiQuery4PopularCountryTopicsResult(
            long forumId,
            String forumTitle,
            long forumCreationDate,
            long moderatorId,
            int count )
    {
        this.forumId = forumId;
        this.forumTitle = forumTitle;
        this.forumCreationDate = forumCreationDate;
        this.moderatorId = moderatorId;
        this.count = count;
    }

    public long forumId()
    {
        return forumId;
    }

    public String forumTitle()
    {
        return forumTitle;
    }

    public long forumCreationDate()
    {
        return forumCreationDate;
    }

    public long moderatorId()
    {
        return moderatorId;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery4PopularCountryTopicsResult{" +
               "forumId=" + forumId +
               ", forumTitle='" + forumTitle + '\'' +
               ", forumCreationDate=" + forumCreationDate +
               ", moderatorId=" + moderatorId +
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

        LdbcSnbBiQuery4PopularCountryTopicsResult that = (LdbcSnbBiQuery4PopularCountryTopicsResult) o;

        if ( forumId != that.forumId )
        { return false; }
        if ( forumCreationDate != that.forumCreationDate )
        { return false; }
        if ( moderatorId != that.moderatorId )
        { return false; }
        if ( count != that.count )
        { return false; }
        return !(forumTitle != null ? !forumTitle.equals( that.forumTitle ) : that.forumTitle != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (forumTitle != null ? forumTitle.hashCode() : 0);
        result = 31 * result + (int) (forumCreationDate ^ (forumCreationDate >>> 32));
        result = 31 * result + (int) (moderatorId ^ (moderatorId >>> 32));
        result = 31 * result + count;
        return result;
    }
}
