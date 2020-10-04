package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.util.Objects;

public class LdbcSnbBiQuery19InteractionPathBetweenCitiesResult
{
    private final long person1Id;
    private final long person2Id;
    private final float totalWeight;

    public LdbcSnbBiQuery19InteractionPathBetweenCitiesResult( long person1Id, long person2Id, float totalWeight )
    {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
        this.totalWeight = totalWeight;
    }

    public long person1Id() {
        return person1Id;
    }

    public long person2Id() {
        return person2Id;
    }

    public float totalWeight() {
        return totalWeight;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery19InteractionPathBetweenCitiesResult{" +
                "person1Id=" + person1Id +
                ", person2Id=" + person2Id +
                ", totalWeight=" + totalWeight +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery19InteractionPathBetweenCitiesResult that = (LdbcSnbBiQuery19InteractionPathBetweenCitiesResult) o;

        if (person1Id != that.person1Id) return false;
        if (person2Id != that.person2Id) return false;
        return Float.compare(that.totalWeight, totalWeight) == 0;
    }

    @Override
    public int hashCode() {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        result = 31 * result + (totalWeight != +0.0f ? Float.floatToIntBits(totalWeight) : 0);
        return result;
    }
}
