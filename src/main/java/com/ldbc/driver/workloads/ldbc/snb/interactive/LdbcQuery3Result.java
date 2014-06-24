package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery3Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long xCount;
    private final long yCount;
    private final long count;

    public LdbcQuery3Result(long personId, String personFirstName, String personLastName, long xCount, long yCount, long count) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.xCount = xCount;
        this.yCount = yCount;
        this.count = count;
    }

    public long personId() {
        return personId;
    }

    public String personFirstName() {
        return personFirstName;
    }

    public String personLastName() {
        return personLastName;
    }

    public long xCount() {
        return xCount;
    }

    public long yCount() {
        return yCount;
    }

    public long count() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery3Result that = (LdbcQuery3Result) o;

        if (count != that.count) return false;
        if (personId != that.personId) return false;
        if (xCount != that.xCount) return false;
        if (yCount != that.yCount) return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery3Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", xCount=" + xCount +
                ", yCount=" + yCount +
                ", count=" + count +
                '}';
    }
}
