package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery12Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final String tagName;
    private final int replyCount;

    public LdbcQuery12Result(long personId, String personFirstName, String personLastName, String tagName, int replyCount) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.tagName = tagName;
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

    public String tagName() {
        return tagName;
    }

    public int replyCount() {
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
        if (tagName != null ? !tagName.equals(that.tagName) : that.tagName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (tagName != null ? tagName.hashCode() : 0);
        result = 31 * result + replyCount;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery12Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", tagName='" + tagName + '\'' +
                ", replyCount=" + replyCount +
                '}';
    }
}
