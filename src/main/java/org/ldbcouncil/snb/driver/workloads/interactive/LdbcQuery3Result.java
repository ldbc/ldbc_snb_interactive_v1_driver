package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcQuery3Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long xCount;
    private final long yCount;
    private final long count;

    public LdbcQuery3Result(
        @JsonProperty("personId")        long personId,
        @JsonProperty("personFirstName") String personFirstName,
        @JsonProperty("personLastName")  String personLastName,
        @JsonProperty("xCount")          long xCount,
        @JsonProperty("yCount")          long yCount,
        @JsonProperty("count")           long count
    )
    {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.xCount = xCount;
        this.yCount = yCount;
        this.count = count;
    }

    public long getPersonId() {
        return personId;
    }

    public String getPersonFirstName() {
        return personFirstName;
    }

    public String getPersonLastName() {
        return personLastName;
    }

    public long getxCount() {
        return xCount;
    }

    public long getyCount() {
        return yCount;
    }

    public long getCount() {
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
