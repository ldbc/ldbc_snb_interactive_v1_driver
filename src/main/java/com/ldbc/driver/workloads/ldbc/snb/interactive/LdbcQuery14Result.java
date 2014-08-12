package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Iterables;

public class LdbcQuery14Result {
    private final Iterable<Long> personIdsInPath;
    private final double pathWeight;

    public LdbcQuery14Result(Iterable<Long> personIdsInPath, double pathWeight) {
        this.personIdsInPath = personIdsInPath;
        this.pathWeight = pathWeight;
    }

    public Iterable<Long> personsIdsInPath() {
        return personIdsInPath;
    }

    public double pathWeight() {
        return pathWeight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery14Result that = (LdbcQuery14Result) o;

        if (Double.compare(that.pathWeight, pathWeight) != 0) return false;
        if (personIdsInPath != null ? !Iterables.elementsEqual(personIdsInPath, that.personIdsInPath) : that.personIdsInPath != null)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery14Result{" +
                "personIdsInPath=" + personIdsInPath +
                ", pathWeight=" + pathWeight +
                '}';
    }
}