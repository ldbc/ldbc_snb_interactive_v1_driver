package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery13Result {
    private final int shortestPathLength;

    public LdbcQuery13Result(int shortestPathLength) {
        this.shortestPathLength = shortestPathLength;
    }

    public int shortestPathLength() {
        return shortestPathLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery13Result that = (LdbcQuery13Result) o;

        if (shortestPathLength != that.shortestPathLength) return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery13Result{" +
                "shortestPathLength=" + shortestPathLength +
                '}';
    }
}
