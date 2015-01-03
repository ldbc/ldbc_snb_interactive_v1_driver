package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;

public class LdbcUpdate8AddFriendship extends Operation<Object> {
    public static final int TYPE = 1008;
    private final long person1Id;
    private final long person2Id;
    private final Date creationDate;

    public LdbcUpdate8AddFriendship(long person1Id, long person2Id, Date creationDate) {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
        this.creationDate = creationDate;
    }

    public long person1Id() {
        return person1Id;
    }

    public long person2Id() {
        return person2Id;
    }

    public Date creationDate() {
        return creationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcUpdate8AddFriendship that = (LdbcUpdate8AddFriendship) o;

        if (person1Id != that.person1Id) return false;
        if (person2Id != that.person2Id) return false;
        if (creationDate != null ? !creationDate.equals(that.creationDate) : that.creationDate != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcUpdate8AddFriendship{" +
                "person1Id=" + person1Id +
                ", person2Id=" + person2Id +
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
        return TYPE;
    }
}
