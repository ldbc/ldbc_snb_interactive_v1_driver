package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.util.Objects;

public class LdbcSnbBiQuery20RecruitmentResult
{
    private final long person1Id;
    private final int totalWeight;

    public LdbcSnbBiQuery20RecruitmentResult(long person1Id, int totalWeight)
    {
        this.person1Id = person1Id;
        this.totalWeight = totalWeight;
    }

    public long person1Id() {
        return person1Id;
    }

    public int totalWeight() {
        return totalWeight;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery20RecruitmentResult{" +
                "person1Id=" + person1Id +
                ", totalWeight=" + totalWeight +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery20RecruitmentResult that = (LdbcSnbBiQuery20RecruitmentResult) o;

        if (person1Id != that.person1Id) return false;
        return totalWeight == that.totalWeight;
    }

    @Override
    public int hashCode() {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + totalWeight;
        return result;
    }
}
