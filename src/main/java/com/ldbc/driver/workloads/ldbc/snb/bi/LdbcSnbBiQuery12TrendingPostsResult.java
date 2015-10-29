package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery12TrendingPostsResult
{
    private final long messageId;
    private final String firstName;
    private final String lastName;
    private final long creationDate;
    private final int likeCount;

    public LdbcSnbBiQuery12TrendingPostsResult(
            long messageId,
            String firstName,
            String lastName,
            long creationDate,
            int likeCount )
    {
        this.messageId = messageId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.creationDate = creationDate;
        this.likeCount = likeCount;
    }

    public long messageId()
    {
        return messageId;
    }

    public String firstName()
    {
        return firstName;
    }

    public String lastName()
    {
        return lastName;
    }

    public long creationDate()
    {
        return creationDate;
    }

    public int likeCount()
    {
        return likeCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery12TrendingPostsResult{" +
               "messageId=" + messageId +
               ", firstName='" + firstName + '\'' +
               ", lastName='" + lastName + '\'' +
               ", creationDate=" + creationDate +
               ", likeCount=" + likeCount +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery12TrendingPostsResult that = (LdbcSnbBiQuery12TrendingPostsResult) o;

        if ( messageId != that.messageId )
        { return false; }
        if ( creationDate != that.creationDate )
        { return false; }
        if ( likeCount != that.likeCount )
        { return false; }
        if ( firstName != null ? !firstName.equals( that.firstName ) : that.firstName != null )
        { return false; }
        return !(lastName != null ? !lastName.equals( that.lastName ) : that.lastName != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (messageId ^ (messageId >>> 32));
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        result = 31 * result + (int) (creationDate ^ (creationDate >>> 32));
        result = 31 * result + likeCount;
        return result;
    }
}
