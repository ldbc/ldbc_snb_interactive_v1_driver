package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcQuery11Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final String organizationName;
    private final int organizationWorkFromYear;

    public LdbcQuery11Result(
        @JsonProperty("personId")                 long personId,
        @JsonProperty("personFirstName")          String personFirstName,
        @JsonProperty("personLastName")           String personLastName,
        @JsonProperty("organizationName")         String organizationName,
        @JsonProperty("organizationWorkFromYear") int organizationWorkFromYear
    )
    {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.organizationName = organizationName;
        this.organizationWorkFromYear = organizationWorkFromYear;
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

    public String getOrganizationName() {
        return organizationName;
    }

    public int getOrganizationWorkFromYear() {
        return organizationWorkFromYear;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery11Result that = (LdbcQuery11Result) o;

        if (organizationWorkFromYear != that.organizationWorkFromYear) return false;
        if (personId != that.personId) return false;
        if (organizationName != null ? !organizationName.equals(that.organizationName) : that.organizationName != null)
            return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery11Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", organizationName='" + organizationName + '\'' +
                ", organizationWorkFromYear=" + organizationWorkFromYear +
                '}';
    }
}
