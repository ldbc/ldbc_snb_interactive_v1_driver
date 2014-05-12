package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;

public class LdbcQuery1Result {
    private final long friendId;
    private final String friendLastName;
    private final int distanceFromPerson;
    private final long friendBirthday;
    private final long friendCreationDate;
    private final String friendGender;
    private final String friendBrowserUsed;
    private final String friendLocationIp;
    private final Set<String> friendEmails;
    private final Set<String> friendLanguages;
    private final String friendCityName;
    // (Person-studyAt->University.name,
    // Person-studyAt->.classYear,
    // Person-studyAt->University-isLocatedIn->City.name)
    private final Set<String> friendUniversities;
    // (Person-workAt->Company.name,
    // Person-workAt->.workFrom,
    // Person-workAt->Company-isLocatedIn->City.name)
    private final Set<String> friendCompanies;

    public LdbcQuery1Result(
            long friendId,
            String friendLastName,
            int distanceFromPerson,
            long friendBirthday,
            long friendCreationDate,
            String friendGender,
            String friendBrowserUsed,
            String friendLocationIp,
            Collection<String> friendEmails,
            Collection<String> friendLanguages,
            String friendCityName,
            Collection<String> friendUniversities,
            Collection<String> friendCompanies) {
        this.friendId = friendId;
        this.friendLastName = friendLastName;
        this.distanceFromPerson = distanceFromPerson;
        this.friendBirthday = friendBirthday;
        this.friendCreationDate = friendCreationDate;
        this.friendGender = friendGender;
        this.friendBrowserUsed = friendBrowserUsed;
        this.friendLocationIp = friendLocationIp;
        this.friendEmails = Sets.newHashSet(friendEmails);
        this.friendLanguages = Sets.newHashSet(friendLanguages);
        this.friendCityName = friendCityName;
        this.friendUniversities = Sets.newHashSet(friendUniversities);
        this.friendCompanies = Sets.newHashSet(friendCompanies);
    }

    public long friendId() {
        return friendId;
    }

    public String friendLastName() {
        return friendLastName;
    }

    public int distanceFromPerson() {
        return distanceFromPerson;
    }

    public long friendBirthday() {
        return friendBirthday;
    }

    public long friendCreationDate() {
        return friendCreationDate;
    }

    public String friendGender() {
        return friendGender;
    }

    public String friendBrowserUsed() {
        return friendBrowserUsed;
    }

    public String friendLocationIp() {
        return friendLocationIp;
    }

    public Set<String> friendEmails() {
        return friendEmails;
    }

    public Set<String> friendLanguages() {
        return friendLanguages;
    }

    public String friendCityName() {
        return friendCityName;
    }

    public Set<String> friendUniversities() {
        return friendUniversities;
    }

    public Set<String> friendCompanies() {
        return friendCompanies;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery1Result that = (LdbcQuery1Result) o;

        if (distanceFromPerson != that.distanceFromPerson) return false;
        if (friendBirthday != that.friendBirthday) return false;
        if (friendCreationDate != that.friendCreationDate) return false;
        if (friendId != that.friendId) return false;
        if (friendBrowserUsed != null ? !friendBrowserUsed.equals(that.friendBrowserUsed) : that.friendBrowserUsed != null)
            return false;
        if (friendCityName != null ? !friendCityName.equals(that.friendCityName) : that.friendCityName != null)
            return false;
        if (friendCompanies != null ? !friendCompanies.equals(that.friendCompanies) : that.friendCompanies != null)
            return false;
        if (friendEmails != null ? !friendEmails.equals(that.friendEmails) : that.friendEmails != null) return false;
        if (friendGender != null ? !friendGender.equals(that.friendGender) : that.friendGender != null) return false;
        if (friendLanguages != null ? !friendLanguages.equals(that.friendLanguages) : that.friendLanguages != null)
            return false;
        if (friendLastName != null ? !friendLastName.equals(that.friendLastName) : that.friendLastName != null)
            return false;
        if (friendLocationIp != null ? !friendLocationIp.equals(that.friendLocationIp) : that.friendLocationIp != null)
            return false;
        if (friendUniversities != null ? !friendUniversities.equals(that.friendUniversities) : that.friendUniversities != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (friendId ^ (friendId >>> 32));
        result = 31 * result + (friendLastName != null ? friendLastName.hashCode() : 0);
        result = 31 * result + distanceFromPerson;
        result = 31 * result + (int) (friendBirthday ^ (friendBirthday >>> 32));
        result = 31 * result + (int) (friendCreationDate ^ (friendCreationDate >>> 32));
        result = 31 * result + (friendGender != null ? friendGender.hashCode() : 0);
        result = 31 * result + (friendBrowserUsed != null ? friendBrowserUsed.hashCode() : 0);
        result = 31 * result + (friendLocationIp != null ? friendLocationIp.hashCode() : 0);
        result = 31 * result + (friendEmails != null ? friendEmails.hashCode() : 0);
        result = 31 * result + (friendLanguages != null ? friendLanguages.hashCode() : 0);
        result = 31 * result + (friendCityName != null ? friendCityName.hashCode() : 0);
        result = 31 * result + (friendUniversities != null ? friendUniversities.hashCode() : 0);
        result = 31 * result + (friendCompanies != null ? friendCompanies.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery1Result{" +
                "friendId=" + friendId +
                ", friendLastName='" + friendLastName + '\'' +
                ", distanceFromPerson=" + distanceFromPerson +
                ", friendBirthday=" + friendBirthday +
                ", friendCreationDate=" + friendCreationDate +
                ", friendGender='" + friendGender + '\'' +
                ", friendBrowserUsed='" + friendBrowserUsed + '\'' +
                ", friendLocationIp='" + friendLocationIp + '\'' +
                ", friendEmails=" + friendEmails +
                ", friendLanguages=" + friendLanguages +
                ", friendCityName='" + friendCityName + '\'' +
                ", friendUniversities=" + friendUniversities +
                ", friendCompanies=" + friendCompanies +
                '}';
    }
}
