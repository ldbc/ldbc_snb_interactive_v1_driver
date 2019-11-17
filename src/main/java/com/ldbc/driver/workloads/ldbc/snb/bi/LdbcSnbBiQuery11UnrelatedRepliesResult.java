package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery11UnrelatedRepliesResult
{
    private final long personId;
    private final String tagName;
    private final int likeCount;
    private final int replyCount;

    public LdbcSnbBiQuery11UnrelatedRepliesResult(long personId, String tagName, int likeCount, int replyCount )
    {
        this.personId = personId;
        this.tagName = tagName;
        this.likeCount = likeCount;
        this.replyCount = replyCount;
    }

    public long personId()
    {
        return personId;
    }

    public String tag()
    {
        return tagName;
    }

    public int likeCount()
    {
        return likeCount;
    }

    public int replyCount()
    {
        return replyCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery11UnrelatedRepliesResult{" +
               "personId=" + personId +
               ", tagName='" + tagName + '\'' +
               ", likeCount=" + likeCount +
               ", replyCount=" + replyCount +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery11UnrelatedRepliesResult that = (LdbcSnbBiQuery11UnrelatedRepliesResult) o;

        if ( personId != that.personId )
        { return false; }
        if ( likeCount != that.likeCount )
        { return false; }
        if ( replyCount != that.replyCount )
        { return false; }
        return !(tagName != null ? !tagName.equals( that.tagName) : that.tagName != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (tagName != null ? tagName.hashCode() : 0);
        result = 31 * result + likeCount;
        result = 31 * result + replyCount;
        return result;
    }
}
