package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery14Result {
    private final Iterable<Long> personsIdsInPath;
    private final double pathWeight;

    public LdbcQuery14Result(Iterable<Long> personsIdsInPath, double pathWeight) {
        this.personsIdsInPath = personsIdsInPath;
        this.pathWeight = pathWeight;
    }

    public Iterable<Long> personsIdsInPath() {
        return personsIdsInPath;
    }

    public double pathWeight() {
        return pathWeight;
    }

    @Override
    public String toString() {
        return "LdbcQuery14Result{" +
                "personsIdsInPath=" + personsIdsInPath +
                ", pathWeight=" + pathWeight +
                '}';
    }
}