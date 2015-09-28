package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery12TrendingPostsResult
{
    private final long postId;
    private final String firstName;
    private final String lastName;
    private final long creationDate;
    private final int count;

    public LdbcSnbBiQuery12TrendingPostsResult( long postId, String firstName, String lastName, long creationDate,
            int count )
    {
        this.postId = postId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.creationDate = creationDate;
        this.count = count;
    }

    public long postId()
    {
        return postId;
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

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery12Result{" +
               "postId=" + postId +
               ", firstName='" + firstName + '\'' +
               ", lastName='" + lastName + '\'' +
               ", creationDate=" + creationDate +
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

        LdbcSnbBiQuery12TrendingPostsResult that = (LdbcSnbBiQuery12TrendingPostsResult) o;

        if ( postId != that.postId )
        { return false; }
        if ( creationDate != that.creationDate )
        { return false; }
        if ( count != that.count )
        { return false; }
        if ( firstName != null ? !firstName.equals( that.firstName ) : that.firstName != null )
        { return false; }
        return !(lastName != null ? !lastName.equals( that.lastName ) : that.lastName != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (postId ^ (postId >>> 32));
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        result = 31 * result + (int) (creationDate ^ (creationDate >>> 32));
        result = 31 * result + count;
        return result;
    }
}
