package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.util.List;

public class LdbcQuery14Result {
    // starting with (and including) ID/URL of start person, ending with (and including) ID/URL of end person, must be in correct order
    private final List<Long> nodesInShortestPath;

    public LdbcQuery14Result(List<Long> nodesInShortestPath) {
        this.nodesInShortestPath = nodesInShortestPath;
    }

    public List<Long> nodesInShortestPath() {
        return nodesInShortestPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery14Result that = (LdbcQuery14Result) o;

        if (nodesInShortestPath != null ? !nodesInShortestPath.equals(that.nodesInShortestPath) : that.nodesInShortestPath != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return nodesInShortestPath != null ? nodesInShortestPath.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "LdbcQuery14Result{" +
                "nodesInShortestPath=" + nodesInShortestPath +
                '}';
    }
}
