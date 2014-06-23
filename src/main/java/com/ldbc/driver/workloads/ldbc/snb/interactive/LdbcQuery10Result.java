package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery10Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final String gender;
    private final String personCityName;
    private final double commonInterestScore;

    public LdbcQuery10Result(String personFirstName, String personLastName, long personId, double commonInterestScore, String gender, String personCityName) {
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.personId = personId;
        this.commonInterestScore = commonInterestScore;
        this.gender = gender;
        this.personCityName = personCityName;
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

    public String gender() {
        return gender;
    }

    public String personCityName() {
        return personCityName;
    }

    public double commonInterestScore() {
        return commonInterestScore;
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
