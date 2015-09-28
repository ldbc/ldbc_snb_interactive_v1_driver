package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery4PopularCountryTopicsResult
{
    private final long forumId;
    private final String title;
    private final long creationDate;
    private final long moderator;
    private final int count;

    public LdbcSnbBiQuery4PopularCountryTopicsResult(
            long forumId,
            String title,
            long creationDate,
            long moderator,
            int count )
    {
        this.forumId = forumId;
        this.title = title;
        this.creationDate = creationDate;
        this.moderator = moderator;
        this.count = count;
    }

    public long forumId()
    {
        return forumId;
    }

    public String title()
    {
        return title;
    }

    public long creationDate()
    {
        return creationDate;
    }

    public long moderator()
    {
        return moderator;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery4Result{" +
               "forumId=" + forumId +
               ", title='" + title + '\'' +
               ", creationDate=" + creationDate +
               ", moderator=" + moderator +
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
        if ( creationDate != that.creationDate )
        { return false; }
        if ( moderator != that.moderator )
        { return false; }
        if ( count != that.count )
        { return false; }
        return !(title != null ? !title.equals( that.title ) : that.title != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (int) (creationDate ^ (creationDate >>> 32));
        result = 31 * result + (int) (moderator ^ (moderator >>> 32));
        result = 31 * result + count;
        return result;
    }
}
