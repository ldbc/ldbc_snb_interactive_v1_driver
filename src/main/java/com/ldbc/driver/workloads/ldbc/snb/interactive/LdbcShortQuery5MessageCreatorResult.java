package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery5MessageCreatorResult {
    private final long personId;
    private final String firstName;
    private final String lastName;
    private final long messageCreationDate;

    public LdbcShortQuery5MessageCreatorResult(long personId, String firstName, String lastName, long messageCreationDate) {
        this.personId = personId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.messageCreationDate = messageCreationDate;
    }

    public long personId() {
        return personId;
    }

    public String firstName() {
        return firstName;
    }

    public String lastName() {
        return lastName;
    }

    public long messageCreationDate() {
        return messageCreationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery5MessageCreatorResult that = (LdbcShortQuery5MessageCreatorResult) o;

        if (messageCreationDate != that.messageCreationDate) return false;
        if (personId != that.personId) return false;
        if (firstName != null ? !firstName.equals(that.firstName) : that.firstName != null) return false;
        if (lastName != null ? !lastName.equals(that.lastName) : that.lastName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        result = 31 * result + (int) (messageCreationDate ^ (messageCreationDate >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery5MessageCreatorResult{" +
                "personId=" + personId +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", messageCreationDate=" + messageCreationDate +
                '}';
    }
}