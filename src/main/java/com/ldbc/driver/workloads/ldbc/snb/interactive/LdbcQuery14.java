package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery14 extends Operation<List<LdbcQuery14Result>> {
    public static final int DEFAULT_LIMIT = 10;

    private final long person1Id;
    private final long person2Id;
    private final String person1Uri;
    private final String person2Uri;
    private final int limit;

    public LdbcQuery14(long person1Id, String person1Uri, long person2Id, String person2Uri, int limit) {
        this.person1Id = person1Id;
        this.person2Id = person2Id;
        this.person1Uri = person1Uri;
        this.person2Uri = person2Uri;
        this.limit = limit;
    }

    public long person1Id() {
        return person1Id;
    }

    public long person2Id() {
        return person2Id;
    }

    public String person1Uri() {
        return person1Uri;
    }

    public String person2Uri() {
        return person2Uri;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery14 that = (LdbcQuery14) o;

        if (limit != that.limit) return false;
        if (person1Id != that.person1Id) return false;
        if (person2Id != that.person2Id) return false;
        if (person1Uri != null ? !person1Uri.equals(that.person1Uri) : that.person1Uri != null) return false;
        if (person2Uri != null ? !person2Uri.equals(that.person2Uri) : that.person2Uri != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + (int) (person2Id ^ (person2Id >>> 32));
        result = 31 * result + (person1Uri != null ? person1Uri.hashCode() : 0);
        result = 31 * result + (person2Uri != null ? person2Uri.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery14{" +
                "person1Id=" + person1Id +
                ", person2Id=" + person2Id +
                ", person1Uri='" + person1Uri + '\'' +
                ", person2Uri='" + person2Uri + '\'' +
                ", limit=" + limit +
                '}';
    }
}
