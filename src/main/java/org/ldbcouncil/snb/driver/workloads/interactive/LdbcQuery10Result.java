package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcQuery10Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final int commonInterestScore;
    private final String personGender;
    private final String personCityName;

    public LdbcQuery10Result(
        @JsonProperty("personId")            long personId,
        @JsonProperty("personFirstName")     String personFirstName,
        @JsonProperty("personLastName")      String personLastName,
        @JsonProperty("commonInterestScore") int commonInterestScore,
        @JsonProperty("personGender")        String personGender,
        @JsonProperty("personCityName")      String personCityName
    )
    {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.commonInterestScore = commonInterestScore;
        this.personGender = personGender;
        this.personCityName = personCityName;
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

    public String getPersonGender() {
        return personGender;
    }

    public String getPersonCityName() {
        return personCityName;
    }

    public int getCommonInterestScore() {
        return commonInterestScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery10Result that = (LdbcQuery10Result) o;

        if (commonInterestScore != that.commonInterestScore) return false;
        if (personId != that.personId) return false;
        if (personCityName != null ? !personCityName.equals(that.personCityName) : that.personCityName != null)
            return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personGender != null ? !personGender.equals(that.personGender) : that.personGender != null) return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + commonInterestScore;
        result = 31 * result + (personGender != null ? personGender.hashCode() : 0);
        result = 31 * result + (personCityName != null ? personCityName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery10Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", personGender='" + personGender + '\'' +
                ", personCityName='" + personCityName + '\'' +
                ", commonInterestScore=" + commonInterestScore +
                '}';
    }
}
