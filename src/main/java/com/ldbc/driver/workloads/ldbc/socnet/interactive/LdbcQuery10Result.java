package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery10Result {
    private final String personFirstName;
    private final String personLastName;
    private final long personId;
    private final int count;
    private final String gender;

    public LdbcQuery10Result(String personFirstName, String personLastName, long personId, int count, String gender) {
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.personId = personId;
        this.count = count;
        this.gender = gender;
    }

    public String personFirstName() {
        return personFirstName;
    }

    public String personLastName() {
        return personLastName;
    }

    public long personId() {
        return personId;
    }

    public int count() {
        return count;
    }

    public String gender() {
        return gender;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery10Result that = (LdbcQuery10Result) o;

        if (count != that.count) return false;
        if (personId != that.personId) return false;
        if (gender != null ? !gender.equals(that.gender) : that.gender != null) return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = personFirstName != null ? personFirstName.hashCode() : 0;
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        result = 31 * result + count;
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery10Result{" +
                "personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", personId=" + personId +
                ", count=" + count +
                ", gender='" + gender + '\'' +
                '}';
    }
}
