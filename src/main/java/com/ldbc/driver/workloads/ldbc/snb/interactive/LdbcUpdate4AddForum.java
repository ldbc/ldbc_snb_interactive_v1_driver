package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;

import java.util.Arrays;
import java.util.Date;

public class LdbcUpdate4AddForum extends Operation<Object> {
    private final long forumId;
    private final String forumTitle;
    private final Date creationDate;
    private final long moderatorPersonId;
    private final long[] tagIds;

    public LdbcUpdate4AddForum(long forumId, String forumTitle, Date creationDate, long moderatorPersonId, long[] tagIds) {
        this.forumId = forumId;
        this.forumTitle = forumTitle;
        this.creationDate = creationDate;
        this.moderatorPersonId = moderatorPersonId;
        this.tagIds = tagIds;
    }

    public long forumId() {
        return forumId;
    }

    public String forumTitle() {
        return forumTitle;
    }

    public Date creationDate() {
        return creationDate;
    }

    public long moderatorPersonId() {
        return moderatorPersonId;
    }

    public long[] tagIds() {
        return tagIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcUpdate4AddForum that = (LdbcUpdate4AddForum) o;

        if (forumId != that.forumId) return false;
        if (moderatorPersonId != that.moderatorPersonId) return false;
        if (creationDate != null ? !creationDate.equals(that.creationDate) : that.creationDate != null) return false;
        if (forumTitle != null ? !forumTitle.equals(that.forumTitle) : that.forumTitle != null) return false;
        if (!Arrays.equals(tagIds, that.tagIds)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (forumTitle != null ? forumTitle.hashCode() : 0);
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        result = 31 * result + (int) (moderatorPersonId ^ (moderatorPersonId >>> 32));
        result = 31 * result + (tagIds != null ? Arrays.hashCode(tagIds) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcUpdate4AddForum{" +
                "forumId=" + forumId +
                ", forumTitle='" + forumTitle + '\'' +
                ", creationDate=" + creationDate +
                ", moderatorPersonId=" + moderatorPersonId +
                ", tagIds=" + Arrays.toString(tagIds) +
                '}';
    }
}
