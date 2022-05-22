package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcUpdate1AddPerson.java
 * 
 * Interactive workload insert query 1:
 * -- Add person --
 * 
 * Add a Person node, connected to the network by 4 possible edge types.
 */

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.util.ListUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class LdbcUpdate1AddPerson extends Operation<LdbcNoResult>
{
    public static final int TYPE = 1001;
    public static final String PERSON_ID = "personId";
    public static final String PERSON_FIRST_NAME = "personFirstName";
    public static final String PERSON_LAST_NAME = "personLastName";
    public static final String GENDER = "gender";
    public static final String BIRTHDAY = "birthday";
    public static final String CREATION_DATE = "creationDate";
    public static final String LOCATION_IP = "locationIP";
    public static final String BROWSER_USED = "browserUsed";
    public static final String CITY_ID = "cityId";
    public static final String LANGUAGES = "languages";
    public static final String EMAILS = "emails";
    public static final String TAG_IDS = "tagIds";
    public static final String STUDY_AT = "studyAt";
    public static final String WORK_AT = "workAt";

    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final String gender;
    private final Date birthday; // input format "1984-03-22"
    private final Date creationDate; // input format "2004-03-22"
    private final String locationIp;
    private final String browserUsed;
    private final long cityId;
    private final List<String> languages;
    private final List<String> emails;
    private final List<Long> tagIds;
    private final List<Organization> studyAt;
    private final List<Organization> workAt;

    public LdbcUpdate1AddPerson(
        @JsonProperty("personId")        long personId,
        @JsonProperty("personFirstName") String personFirstName,
        @JsonProperty("personLastName")  String personLastName,
        @JsonProperty("gender")          String gender,
        @JsonProperty("birthday")        Date birthday,
        @JsonProperty("creationDate")    Date creationDate,
        @JsonProperty("locationIp")      String locationIp,
        @JsonProperty("browserUsed")     String browserUsed,
        @JsonProperty("cityId")          long cityId,
        @JsonProperty("languages")       List<String> languages,
        @JsonProperty("emails")          List<String> emails,
        @JsonProperty("tagIds")          List<Long> tagIds,
        @JsonProperty("studyAt")         List<Organization> studyAt,
        @JsonProperty("workAt")          List<Organization> workAt
    )
    {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.gender = gender;
        this.birthday = birthday;
        this.creationDate = creationDate;
        this.locationIp = locationIp;
        this.browserUsed = browserUsed;
        this.cityId = cityId;
        this.languages = languages;
        this.emails = emails;
        this.tagIds = tagIds;
        this.studyAt = studyAt;
        this.workAt = workAt;
    }

    public long getPersonId()
    {
        return personId;
    }

    public String getPersonFirstName()
    {
        return personFirstName;
    }

    public String getPersonLastName()
    {
        return personLastName;
    }

    public String getGender()
    {
        return gender;
    }

    public Date getBirthday()
    {
        return birthday;
    }

    public Date getCreationDate()
    {
        return creationDate;
    }

    public String getLocationIp()
    {
        return locationIp;
    }

    public String getBrowserUsed()
    {
        return browserUsed;
    }

    public long getCityId()
    {
        return cityId;
    }

    public List<String> getLanguages()
    {
        return languages;
    }

    public List<String> getEmails()
    {
        return emails;
    }

    public List<Long> getTagIds()
    {
        return tagIds;
    }

    public List<Organization> getStudyAt()
    {
        return studyAt;
    }

    public List<Organization> getWorkAt()
    {
        return workAt;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(PERSON_ID, personId)
                .put(PERSON_FIRST_NAME, personFirstName)
                .put(PERSON_LAST_NAME, personLastName)
                .put(GENDER, gender)
                .put(BIRTHDAY, birthday)
                .put(CREATION_DATE, creationDate)
                .put(LOCATION_IP, locationIp)
                .put(BROWSER_USED, browserUsed)
                .put(CITY_ID, cityId)
                .put(LANGUAGES, languages)
                .put(EMAILS, emails)
                .put(TAG_IDS, tagIds)
                .put(STUDY_AT, studyAt)
                .put(WORK_AT, workAt)
                .build();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcUpdate1AddPerson that = (LdbcUpdate1AddPerson) o;
        if ( cityId != that.cityId )
        { return false; }
        if ( personId != that.personId )
        { return false; }
        if ( birthday != null ? !birthday.equals( that.birthday ) : that.birthday != null )
        { return false; }
        if ( browserUsed != null ? !browserUsed.equals( that.browserUsed ) : that.browserUsed != null )
        { return false; }
        if ( creationDate != null ? !creationDate.equals( that.creationDate ) : that.creationDate != null )
        { return false; }
        if ( emails != null ? !ListUtils.listsEqual( emails ,  that.emails ) : that.emails != null )
        { return false; }
        if ( gender != null ? !gender.equals( that.gender ) : that.gender != null )
        { return false; }
        if ( languages != null ? !ListUtils.listsEqual( languages , that.languages )
                               : that.languages != null )
        { return false; }
        if ( locationIp != null ? !locationIp.equals( that.locationIp ) : that.locationIp != null )
        { return false; }
        if ( personFirstName != null ? !personFirstName.equals( that.personFirstName ) : that.personFirstName != null )
        { return false; }
        if ( personLastName != null ? !personLastName.equals( that.personLastName ) : that.personLastName != null )
        { return false; }
        if ( studyAt != null ? !studyAt.equals( that.studyAt ) : that.studyAt != null )
        { return false; }
        if ( tagIds != null ? !ListUtils.listsEqual( tagIds ,  that.tagIds  ) : that.tagIds != null )
        { return false; }
        if ( workAt != null ? !workAt.equals( that.workAt ) : that.workAt != null )
        { return false; }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + (birthday != null ? birthday.hashCode() : 0);
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        result = 31 * result + (locationIp != null ? locationIp.hashCode() : 0);
        result = 31 * result + (browserUsed != null ? browserUsed.hashCode() : 0);
        result = 31 * result + (int) (cityId ^ (cityId >>> 32));
        result = 31 * result + (languages != null ? languages.hashCode() : 0);
        result = 31 * result + (emails != null ? emails.hashCode() : 0);
        result = 31 * result + (tagIds != null ? tagIds.hashCode() : 0);
        result = 31 * result + (studyAt != null ? studyAt.hashCode() : 0);
        result = 31 * result + (workAt != null ? workAt.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "LdbcUpdate1AddPerson{" +
               "personId=" + personId +
               ", personFirstName='" + personFirstName + '\'' +
               ", personLastName='" + personLastName + '\'' +
               ", gender='" + gender + '\'' +
               ", birthday=" + birthday +
               ", creationDate=" + creationDate +
               ", locationIp='" + locationIp + '\'' +
               ", browserUsed='" + browserUsed + '\'' +
               ", cityId=" + cityId +
               ", languages=" + languages +
               ", emails=" + emails +
               ", tagIds=" + tagIds +
               ", studyAt=" + studyAt +
               ", workAt=" + workAt +
               '}';
    }

    public static class Organization
    {
        private final long organizationId;
        private final int year;

        public Organization(
            @JsonProperty("organizationId") long organizationId,
            @JsonProperty("year")           int year
        )
        {
            this.organizationId = organizationId;
            this.year = year;
        }

        public long getOrganizationId()
        {
            return organizationId;
        }

        public int getYear()
        {
            return year;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            Organization that = (Organization) o;

            if ( organizationId != that.organizationId )
            { return false; }
            if ( year != that.year )
            { return false; }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = (int) (organizationId ^ (organizationId >>> 32));
            result = 31 * result + year;
            return result;
        }

        @Override
        public String toString()
        {
            return "Organization{" +
                   "organizationId=" + organizationId +
                   ", year=" + year +
                   '}';
        }
    }

    @Override
    public LdbcNoResult deserializeResult( String serializedResults )
    {
        return LdbcNoResult.INSTANCE;
    }

    @Override
    public int type()
    {
        return TYPE;
    }
}
