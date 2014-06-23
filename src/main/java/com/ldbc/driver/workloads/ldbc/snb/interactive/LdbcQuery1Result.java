package com.ldbc.driver.workloads.ldbc.snb.interactive;

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
