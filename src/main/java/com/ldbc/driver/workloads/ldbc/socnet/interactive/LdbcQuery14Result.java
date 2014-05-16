package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.util.Collection;

public class LdbcQuery14Result {
    private final Collection<PathNode> pathNodes;
    private final double weight;

    public static class PathNode {
        private final String entityType;
        private final long entityId;

        public PathNode(String entityType, long entityId) {
            this.entityType = entityType;
            this.entityId = entityId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PathNode pathNode = (PathNode) o;

            if (entityId != pathNode.entityId) return false;
            if (entityType != null ? !entityType.equals(pathNode.entityType) : pathNode.entityType != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = entityType != null ? entityType.hashCode() : 0;
            result = 31 * result + (int) (entityId ^ (entityId >>> 32));
            return result;
        }
    }

    public LdbcQuery14Result(Collection<PathNode> pathNodes, double weight) {
        this.pathNodes = pathNodes;
        this.weight = weight;
    }

    public Collection<PathNode> pathNodes() {
        return pathNodes;
    }

    public double weight() {
        return weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery14Result that = (LdbcQuery14Result) o;

        if (Double.compare(that.weight, weight) != 0) return false;
        if (pathNodes != null ? !pathNodes.equals(that.pathNodes) : that.pathNodes != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = pathNodes != null ? pathNodes.hashCode() : 0;
        temp = Double.doubleToLongBits(weight);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery14Result{" +
                "pathNodes=" + pathNodes +
                ", weight=" + weight +
                '}';
    }
}
