package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery1PersonProfileResult {
    private final String firstName;
    private final String lastName;
    private final long birthday;
    private final String locationIp;
    private final String browserUsed;
    private final long cityId;

    public LdbcShortQuery1PersonProfileResult(String firstName,
                                              String lastName,
                                              long birthday,
                                              String locationIp,
                                              String browserUsed,
                                              long cityId) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.birthday = birthday;
        this.locationIp = locationIp;
        this.browserUsed = browserUsed;
        this.cityId = cityId;
    }

    public String firstName() {
        return firstName;
    }

    public String lastName() {
        return lastName;
    }

    public long birthday() {
        return birthday;
    }

    public String locationIp() {
        return locationIp;
    }

    public String browserUsed() {
        return browserUsed;
    }

    public long cityId() {
        return cityId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery1PersonProfileResult that = (LdbcShortQuery1PersonProfileResult) o;

        if (birthday != that.birthday) return false;
        if (cityId != that.cityId) return false;
        if (browserUsed != null ? !browserUsed.equals(that.browserUsed) : that.browserUsed != null) return false;
        if (firstName != null ? !firstName.equals(that.firstName) : that.firstName != null) return false;
        if (lastName != null ? !lastName.equals(that.lastName) : that.lastName != null) return false;
        if (locationIp != null ? !locationIp.equals(that.locationIp) : that.locationIp != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = firstName != null ? firstName.hashCode() : 0;
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        result = 31 * result + (int) (birthday ^ (birthday >>> 32));
        result = 31 * result + (locationIp != null ? locationIp.hashCode() : 0);
        result = 31 * result + (browserUsed != null ? browserUsed.hashCode() : 0);
        result = 31 * result + (int) (cityId ^ (cityId >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery1PersonProfileResult{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", birthday=" + birthday +
                ", locationIp='" + locationIp + '\'' +
                ", browserUsed='" + browserUsed + '\'' +
                ", cityId=" + cityId +
                '}';
    }
}