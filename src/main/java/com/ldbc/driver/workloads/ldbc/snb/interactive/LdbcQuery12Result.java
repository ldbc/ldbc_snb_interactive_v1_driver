package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery12Result {
    /*
    Person.id
Person.firstName
Person.lastName
{Tag.name}
count // number of reply Comments
     */

    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final Iterable<String> tagNames;
    private final long replyCount;

    public LdbcQuery12Result(long personId, String personFirstName, String personLastName, Iterable<String> tagNames, long replyCount) {
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

    public Iterable<String> tagNames() {
        return tagNames;
    }

    public long replyCount() {
        return replyCount;
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
