package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;

public class LdbcUpdate3AddCommentLike extends Operation<Object> {
    private final long personId;
    private final long commentId;
    private final Date creationDate;

    public LdbcUpdate3AddCommentLike(long personId, long commentId, Date creationDate) {
        this.personId = personId;
        this.commentId = commentId;
        this.creationDate = creationDate;
    }

    public long personId() {
        return personId;
    }

    public long commentId() {
        return commentId;
    }

    public Date creationDate() {
        return creationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcUpdate3AddCommentLike that = (LdbcUpdate3AddCommentLike) o;

        if (commentId != that.commentId) return false;
        if (personId != that.personId) return false;
        if (creationDate != null ? !creationDate.equals(that.creationDate) : that.creationDate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (int) (commentId ^ (commentId >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcUpdate3AddCommentLike{" +
                "personId=" + personId +
                ", commentId=" + commentId +
                ", creationDate=" + creationDate +
                '}';
    }

    @Override
    public Object marshalResult(String serializedOperationResult) {
        return null;
    }

    @Override
    public String serializeResult(Object operationResultInstance) {
        return null;
    }

    @Override
    public int type() {
        return 1003;
    }
}
