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
    public String toString() {
        return "LdbcQuery13Result{" +
                "shortestPathLength=" + shortestPathLength +
                '}';
    }
}
