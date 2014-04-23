package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;

public class LdbcUpdate2AddPostLike extends Operation<Object> {
    private final long personId;
    private final long postId;
    private final Date creationDate;

    public LdbcUpdate2AddPostLike(long personId, long postId, Date creationDate) {
        this.personId = personId;
        this.postId = postId;
        this.creationDate = creationDate;
    }

    public long personId() {
        return personId;
    }

    public long postId() {
        return postId;
    }

    public Date creationDate() {
        return creationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcUpdate2AddPostLike that = (LdbcUpdate2AddPostLike) o;

        if (personId != that.personId) return false;
        if (postId != that.postId) return false;
        if (creationDate != null ? !creationDate.equals(that.creationDate) : that.creationDate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (int) (postId ^ (postId >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcUpdate2AddPostLike{" +
                "personId=" + personId +
                ", postId=" + postId +
                ", creationDate=" + creationDate +
                '}';
    }
}
