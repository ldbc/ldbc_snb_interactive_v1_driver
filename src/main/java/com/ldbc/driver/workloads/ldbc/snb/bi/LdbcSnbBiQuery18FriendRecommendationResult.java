package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.util.List;
import java.util.Objects;

public class LdbcSnbBiQuery18FriendRecommendationResult
{
    private final long person2Id;
    private final int mutualFriendCount;

    public LdbcSnbBiQuery18FriendRecommendationResult( long person2Id, int mutualFriendCount )
    {
        this.person2Id = person2Id;
        this.mutualFriendCount = mutualFriendCount;
    }

    public long person2Id() {
        return person2Id;
    }

    public int mutualFriendCount() {
        return mutualFriendCount;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery18FriendRecommendationResult{" +
                "person2Id=" + person2Id +
                ", mutualFriendCount=" + mutualFriendCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LdbcSnbBiQuery18FriendRecommendationResult that = (LdbcSnbBiQuery18FriendRecommendationResult) o;
        return person2Id == that.person2Id &&
                mutualFriendCount == that.mutualFriendCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(person2Id, mutualFriendCount);
    }
}
