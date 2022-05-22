package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.ldbcouncil.snb.driver.util.ListUtils;

public class LdbcQuery1Result {
    private final long friendId;
    private final String friendLastName;
    private final int distanceFromPerson;
    private final long friendBirthday;
    private final long friendCreationDate;
    private final String friendGender;
    private final String friendBrowserUsed;
    private final String friendLocationIp;
    private final Iterable<String> friendEmails;
    private final Iterable<String> friendLanguages;
    private final String friendCityName;
    // (Person-studyAt->University.name,
    // Person-studyAt->.classYear,
    // Person-studyAt->University-isLocatedIn->City.name)
    private final Iterable<Organization> friendUniversities;
    // (Person-workAt->Company.name,
    // Person-workAt->.workFrom,
    // Person-workAt->Company-isLocatedIn->City.name)
    private final Iterable<Organization> friendCompanies;

    public LdbcQuery1Result(
        @JsonProperty("friendId")           long friendId,
        @JsonProperty("friendLastName")     String friendLastName,
        @JsonProperty("distanceFromPerson") int distanceFromPerson,
        @JsonProperty("friendBirthday")     long friendBirthday,
        @JsonProperty("friendCreationDate") long friendCreationDate,
        @JsonProperty("friendGender")       String friendGender,
        @JsonProperty("friendBrowserUsed")  String friendBrowserUsed,
        @JsonProperty("friendLocationIp")   String friendLocationIp,
        @JsonProperty("friendEmails")       Iterable<String> friendEmails,
        @JsonProperty("friendLanguages")    Iterable<String> friendLanguages,
        @JsonProperty("friendCityName")     String friendCityName,
        @JsonProperty("friendUniversities") Iterable<Organization> friendUniversities,
        @JsonProperty("friendCompanies")    Iterable<Organization> friendCompanies
    )
    {
        this.friendId = friendId;
        this.friendLastName = friendLastName;
        this.distanceFromPerson = distanceFromPerson;
        this.friendBirthday = friendBirthday;
        this.friendCreationDate = friendCreationDate;
        this.friendGender = friendGender;
        this.friendBrowserUsed = friendBrowserUsed;
        this.friendLocationIp = friendLocationIp;
        this.friendEmails = friendEmails;
        this.friendLanguages = friendLanguages;
        this.friendCityName = friendCityName;
        this.friendUniversities = friendUniversities;
        this.friendCompanies = friendCompanies;
    }

    public long getFriendId() {
        return friendId;
    }

    public String getFriendLastName() {
        return friendLastName;
    }

    public int getDistanceFromPerson() {
        return distanceFromPerson;
    }

    public long getFriendBirthday() {
        return friendBirthday;
    }

    public long getFriendCreationDate() {
        return friendCreationDate;
    }

    public String getFriendGender() {
        return friendGender;
    }

    public String getFriendBrowserUsed() {
        return friendBrowserUsed;
    }

    public String getFriendLocationIp() {
        return friendLocationIp;
    }

    public Iterable<String> getFriendEmails() {
        return friendEmails;
    }

    public Iterable<String> getFriendLanguages() {
        return friendLanguages;
    }

    public String getFriendCityName() {
        return friendCityName;
    }

    public Iterable<Organization> getFriendUniversities() {
        return friendUniversities;
    }

    public Iterable<Organization> getFriendCompanies() {
        return friendCompanies;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery1Result other = (LdbcQuery1Result) o;

        if (distanceFromPerson != other.distanceFromPerson) return false;
        if (friendBirthday != other.friendBirthday) return false;
        if (friendCreationDate != other.friendCreationDate) return false;
        if (friendId != other.friendId) return false;
        if (friendBrowserUsed != null ? !friendBrowserUsed.equals(other.friendBrowserUsed) : other.friendBrowserUsed != null)
            return false;
        if (friendCityName != null ? !friendCityName.equals(other.friendCityName) : other.friendCityName != null)
            return false;
        if ((null == friendCompanies ^ null == other.friendCompanies) || !ListUtils.listsEqual(friendCompanies, other.friendCompanies))
            return false;
        if (friendEmails != null ? !ListUtils.listsEqual(friendEmails, other.friendEmails) : other.friendEmails != null)
            return false;
        if (friendGender != null ? !friendGender.equals(other.friendGender) : other.friendGender != null)
            return false;
        if (friendLanguages != null ? !ListUtils.listsEqual(friendLanguages, other.friendLanguages) : other.friendLanguages != null)
            return false;
        if (friendLastName != null ? !friendLastName.equals(other.friendLastName) : other.friendLastName != null)
            return false;
        if (friendLocationIp != null ? !friendLocationIp.equals(other.friendLocationIp) : other.friendLocationIp != null)
            return false;
        if (friendUniversities != null ? !ListUtils.listsEqual(friendUniversities, other.friendUniversities) : other.friendUniversities != null)
            return false;
        return true;
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


    /**
     * Inner class to store Organization result information.
     */
    public static class Organization
    {
        private final String organizationName;
        private final int year;
        private final String placeName;

        public Organization(
            @JsonProperty("organizationName") String organizationName,
            @JsonProperty("year")             int year,
            @JsonProperty("placeName")        String placeName
        )
        {
            this.organizationName = organizationName;
            this.year = year;
            this.placeName = placeName;
        }

        public String getOrganizationName()
        {
            return organizationName;
        }

        public int getYear()
        {
            return year;
        }

        public String getPlaceName()
        {
            return placeName;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            Organization that = (Organization) o;

            if ( !organizationName.equals(that.organizationName ))
            { return false; }
            if ( year != that.year )
            { return false; }
            if (! placeName.equals(that.placeName ))
            { return false; }
            return true;
        }

        @Override
        public int hashCode()
        {
            int result = (organizationName != null ? organizationName.hashCode() : 0);
            result = 31 * result + (placeName != null ? placeName.hashCode() : 0);
            result = 31 * result + year;
            return result;
        }

        @Override
        public String toString()
        {
            return "Organization{" +
                   "organizationName=" + organizationName +
                   ", year=" + year +
                   ", placeName=" + placeName +
                   '}';
        }
    }
}
