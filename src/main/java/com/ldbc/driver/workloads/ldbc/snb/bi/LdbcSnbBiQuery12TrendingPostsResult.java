package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery12TrendingPostsResult
{
    private final long messageId;
    private final long messageCreationDate;
    private final String creatorFirstName;
    private final String creatorLastName;
    private final int likeCount;

    public LdbcSnbBiQuery12TrendingPostsResult(
            long messageId,
            long messageCreationDate,
            String creatorFirstName,
            String creatorLastName,
            int likeCount )
    {
        this.messageId = messageId;
        this.messageCreationDate = messageCreationDate;
        this.creatorFirstName = creatorFirstName;
        this.creatorLastName = creatorLastName;
        this.likeCount = likeCount;
    }

    public long messageId()
    {
        return messageId;
    }

    public long messageCreationDate()
    {
        return messageCreationDate;
    }

    public String creatorFirstName()
    {
        return creatorFirstName;
    }

    public String creatorLastName()
    {
        return creatorLastName;
    }

    public int likeCount()
    {
        return likeCount;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery12TrendingPostsResult{" +
                "messageId=" + messageId +
                ", messageCreationDate=" + messageCreationDate +
                ", creatorFirstName='" + creatorFirstName + '\'' +
                ", creatorLastName='" + creatorLastName + '\'' +
                ", likeCount=" + likeCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery12TrendingPostsResult that = (LdbcSnbBiQuery12TrendingPostsResult) o;

        if (messageId != that.messageId) return false;
        if (messageCreationDate != that.messageCreationDate) return false;
        if (likeCount != that.likeCount) return false;
        if (creatorFirstName != null ? !creatorFirstName.equals(that.creatorFirstName) : that.creatorFirstName != null) return false;
        return creatorLastName != null ? creatorLastName.equals(that.creatorLastName) : that.creatorLastName == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (messageId ^ (messageId >>> 32));
        result = 31 * result + (int) (messageCreationDate ^ (messageCreationDate >>> 32));
        result = 31 * result + (creatorFirstName != null ? creatorFirstName.hashCode() : 0);
        result = 31 * result + (creatorLastName != null ? creatorLastName.hashCode() : 0);
        result = 31 * result + likeCount;
        return result;
    }

}
