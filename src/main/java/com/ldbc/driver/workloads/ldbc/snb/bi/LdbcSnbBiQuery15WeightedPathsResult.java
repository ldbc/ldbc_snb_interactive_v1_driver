package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.util.List;

public class LdbcSnbBiQuery15WeightedPathsResult
{
    private final List<Long> personIds;
    private final double weight;

    public LdbcSnbBiQuery15WeightedPathsResult( List<Long> personIds, double weight )
    {
        this.personIds = personIds;
        this.weight = weight;
    }

    public List<Long> personIds()
    {
        return personIds;
    }

    public double weight() {
        return weight;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery15WeightedPathsResult{" +
                "personIds=" + personIds +
                ", weight=" + weight +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery15WeightedPathsResult that = (LdbcSnbBiQuery15WeightedPathsResult) o;

        if (Double.compare(that.weight, weight) != 0) return false;
        return personIds != null ? personIds.equals(that.personIds) : that.personIds == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = personIds != null ? personIds.hashCode() : 0;
        temp = Double.doubleToLongBits(weight);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
