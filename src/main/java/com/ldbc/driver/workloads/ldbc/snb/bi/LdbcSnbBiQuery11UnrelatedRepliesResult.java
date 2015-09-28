package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery11UnrelatedRepliesResult
{
    private final long personId;
    private final String tag;
    private final int likeCount;
    private final int replyCount;

    public LdbcSnbBiQuery11UnrelatedRepliesResult( long personId, String tag, int likeCount, int replyCount )
    {
        this.personId = personId;
        this.tag = tag;
        this.likeCount = likeCount;
        this.replyCount = replyCount;
    }

    public long personId()
    {
        return personId;
    }

    public String tag()
    {
        return tag;
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
        return "LdbcSnbBiQuery11Result{" +
               "personId=" + personId +
               ", tag='" + tag + '\'' +
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
        return !(tag != null ? !tag.equals( that.tag ) : that.tag != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (tag != null ? tag.hashCode() : 0);
        result = 31 * result + likeCount;
        result = 31 * result + replyCount;
        return result;
    }
}
