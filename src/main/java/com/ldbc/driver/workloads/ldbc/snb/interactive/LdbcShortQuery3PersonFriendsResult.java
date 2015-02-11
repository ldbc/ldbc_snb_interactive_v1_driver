package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery3PersonFriendsResult {
    private final long personId;
    private final String firstName;
    private final String lastName;
    private final long friendshipCreationDate;

    public LdbcShortQuery3PersonFriendsResult(long personId, String firstName, String lastName, long friendshipCreationDate) {
        this.personId = personId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.friendshipCreationDate = friendshipCreationDate;
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

    public long friendshipCreationDate() {
        return friendshipCreationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery3PersonFriendsResult that = (LdbcShortQuery3PersonFriendsResult) o;

        if (friendshipCreationDate != that.friendshipCreationDate) return false;
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
        result = 31 * result + (int) (friendshipCreationDate ^ (friendshipCreationDate >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery3PersonFriendsResult{" +
                "personId=" + personId +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", friendshipCreationDate=" + friendshipCreationDate +
                '}';
    }
}