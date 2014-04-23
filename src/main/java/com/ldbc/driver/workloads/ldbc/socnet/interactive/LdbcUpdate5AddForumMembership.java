package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;

public class LdbcUpdate5AddForumMembership extends Operation<Object> {
    private final long forumId;
    private final long personId;
    private final Date creationDate;

    public LdbcUpdate5AddForumMembership(long forumId, long personId, Date creationDate) {
        this.forumId = forumId;
        this.personId = personId;
        this.creationDate = creationDate;
    }

    public long forumId() {
        return forumId;
    }

    public long personId() {
        return personId;
    }

    public Date creationDate() {
        return creationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcUpdate5AddForumMembership that = (LdbcUpdate5AddForumMembership) o;

        if (forumId != that.forumId) return false;
        if (personId != that.personId) return false;
        if (creationDate != null ? !creationDate.equals(that.creationDate) : that.creationDate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcUpdate5AddForumMembership{" +
                "forumId=" + forumId +
                ", personId=" + personId +
                ", creationDate=" + creationDate +
                '}';
    }
}
