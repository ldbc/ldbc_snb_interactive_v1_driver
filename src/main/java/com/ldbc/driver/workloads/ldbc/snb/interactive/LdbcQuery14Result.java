package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;

import java.util.Iterator;

public class LdbcQuery14Result {
    private final Iterable<? extends Number> personIdsInPath;
    private final double pathWeight;

    public LdbcQuery14Result(Iterable<? extends Number> personIdsInPath, double pathWeight) {
        this.personIdsInPath = personIdsInPath;
        this.pathWeight = pathWeight;
    }

    public Iterable<? extends Number> personsIdsInPath() {
        // force to List, as Guava/Jackson magic changes it to a strange collection that breaks equality somewhere
        // not performance sensitive code path, only used for validation & serialization - not during runs
        return Lists.newArrayList(personIdsInPath);
    }

    public double pathWeight() {
        return pathWeight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LdbcQuery14Result that = (LdbcQuery14Result) o;
        if (Double.compare(that.pathWeight, pathWeight) != 0) {
            return false;
        }
        if (personIdsInPath != null ? !personIdPathsEqual(personIdsInPath, that.personIdsInPath) : that.personIdsInPath != null) {
            return false;
        }
        return true;
    }

    private boolean personIdPathsEqual(Iterable<? extends Number> path1, Iterable<? extends Number> path2) {
        Iterator<? extends Number> path1Iterator = path1.iterator();
        Iterator<? extends Number> path2Iterator = path2.iterator();
        while (path1Iterator.hasNext()) {
            if (false == path2Iterator.hasNext()) return false;
            long path1Id = path1Iterator.next().longValue();
            long path2Id = path2Iterator.next().longValue();
            if (path1Id != path2Id) return false;
        }
        return false == path2Iterator.hasNext();
    }

    @Override
    public String toString() {
        return "LdbcQuery14Result{" +
                "personIdsInPath=" + personIdsInPath +
                ", pathWeight=" + pathWeight +
                '}';
    }
}