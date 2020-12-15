package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery3PopularCountryTopicsResult
{
    private final long forumId;
    private final String forumTitle;
    private final long forumCreationDate;
    private final long personId;
    private final int messageCount;

    public LdbcSnbBiQuery3PopularCountryTopicsResult(
            long forumId,
            String forumTitle,
            long forumCreationDate,
            long personId,
            int messageCount)
    {
        this.forumId = forumId;
        this.forumTitle = forumTitle;
        this.forumCreationDate = forumCreationDate;
        this.personId = personId;
        this.messageCount = messageCount;
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

    public long personId()
    {
        return personId;
    }

    public int messageCount()
    {
        return messageCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery3PopularCountryTopicsResult{" +
               "forumId=" + forumId +
               ", forumTitle='" + forumTitle + '\'' +
               ", forumCreationDate=" + forumCreationDate +
               ", personId=" + personId +
               ", messageCount=" + messageCount +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery3PopularCountryTopicsResult that = (LdbcSnbBiQuery3PopularCountryTopicsResult) o;

        if ( forumId != that.forumId )
        { return false; }
        if ( forumCreationDate != that.forumCreationDate )
        { return false; }
        if ( personId != that.personId)
        { return false; }
        if ( messageCount != that.messageCount)
        { return false; }
        return !(forumTitle != null ? !forumTitle.equals( that.forumTitle ) : that.forumTitle != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (forumTitle != null ? forumTitle.hashCode() : 0);
        result = 31 * result + (int) (forumCreationDate ^ (forumCreationDate >>> 32));
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        result = 31 * result + messageCount;
        return result;
    }
}
