package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery11Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final int organizationWorkFromYear;
    private final String organizationName;

    public LdbcQuery11Result(long personId, String personFirstName, String personLastName, String organizationName, int organizationWorkFromYear) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.organizationName = organizationName;
        this.organizationWorkFromYear = organizationWorkFromYear;
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

    public String organizationName() {
        return organizationName;
    }

    public int organizationWorkFromYear() {
        return organizationWorkFromYear;
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