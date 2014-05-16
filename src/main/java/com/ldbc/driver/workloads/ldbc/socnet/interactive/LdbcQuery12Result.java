package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.util.Collection;

public class LdbcQuery12Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final Collection<String> tagNames;
    private final long replyCount;

    public LdbcQuery12Result(long personId, String personFirstName, String personLastName, Collection<String> tagNames, long replyCount) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.tagNames = tagNames;
        this.replyCount = replyCount;
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

    public Collection<String> tagNames() {
        return tagNames;
    }

    public long replyCount() {
        return replyCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery12Result that = (LdbcQuery12Result) o;

        if (personId != that.personId) return false;
        if (replyCount != that.replyCount) return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;
        if (tagNames != null ? !tagNames.equals(that.tagNames) : that.tagNames != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (tagNames != null ? tagNames.hashCode() : 0);
        result = 31 * result + (int) (replyCount ^ (replyCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery12Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", tagNames=" + tagNames +
                ", replyCount=" + replyCount +
                '}';
    }
}
