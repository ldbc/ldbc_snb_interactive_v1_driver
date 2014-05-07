package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery10Result {
    private final String personFirstName;
    private final String personLastName;
    private final long personId;
    private final double commonInterestScore;
    private final String gender;
    private final String personCityName;

    public LdbcQuery10Result(String personFirstName, String personLastName, long personId, double commonInterestScore, String gender, String personCityName) {
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.personId = personId;
        this.commonInterestScore = commonInterestScore;
        this.gender = gender;
        this.personCityName = personCityName;
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

    public double commonInterestScore() {
        return commonInterestScore;
    }

    public String gender() {
        return gender;
    }

    public String personCityName() {
        return personCityName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery10Result that = (LdbcQuery10Result) o;

        if (Double.compare(that.commonInterestScore, commonInterestScore) != 0) return false;
        if (personId != that.personId) return false;
        if (gender != null ? !gender.equals(that.gender) : that.gender != null) return false;
        if (personCityName != null ? !personCityName.equals(that.personCityName) : that.personCityName != null)
            return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = personFirstName != null ? personFirstName.hashCode() : 0;
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (int) (personId ^ (personId >>> 32));
        temp = Double.doubleToLongBits(commonInterestScore);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + (personCityName != null ? personCityName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery10Result{" +
                "personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", personId=" + personId +
                ", commonInterestScore=" + commonInterestScore +
                ", gender='" + gender + '\'' +
                ", personCityName='" + personCityName + '\'' +
                '}';
    }
}
