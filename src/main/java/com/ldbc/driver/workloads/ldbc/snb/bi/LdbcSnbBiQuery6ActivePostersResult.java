package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery6ActivePostersResult
{
    private final long personId;
    private final int replyCount;
    private final int likeCount;
    private final int messageCount;
    private final int score;

    public LdbcSnbBiQuery6ActivePostersResult(
            long personId,
            int replyCount,
            int likeCount,
            int messageCount,
            int score )
    {
        this.personId = personId;
        this.replyCount = replyCount;
        this.likeCount = likeCount;
        this.messageCount = messageCount;
        this.score = score;
    }

    public long personId()
    {
        return personId;
    }

    public int replyCount()
    {
        return replyCount;
    }

    public int likeCount()
    {
        return likeCount;
    }

    public int messageCount()
    {
        return messageCount;
    }

    public int score()
    {
        return score;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery6ActivePostersResult{" +
                "personId=" + personId +
                ", replyCount=" + replyCount +
                ", likeCount=" + likeCount +
                ", messageCount=" + messageCount +
                ", score=" + score +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery6ActivePostersResult that = (LdbcSnbBiQuery6ActivePostersResult) o;

        if (personId != that.personId) return false;
        if (replyCount != that.replyCount) return false;
        if (likeCount != that.likeCount) return false;
        if (messageCount != that.messageCount) return false;
        return score == that.score;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + replyCount;
        result = 31 * result + likeCount;
        result = 31 * result + messageCount;
        result = 31 * result + score;
        return result;
    }
}
