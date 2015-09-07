package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.util.ListUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;

public class LdbcUpdate1AddPerson extends Operation<LdbcNoResult>
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final int TYPE = 1001;
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

    public LdbcUpdate1AddPerson( long personId,
            String personFirstName,
            String personLastName,
            String gender,
            Date birthday,
            Date creationDate,
            String locationIp,
            String browserUsed,
            long cityId,
            List<String> languages,
            List<String> emails,
            List<Long> tagIds,
            List<Organization> studyAt,
            List<Organization> workAt )
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

    public long personId()
    {
        return personId;
    }

    public String personFirstName()
    {
        return personFirstName;
    }

    public String personLastName()
    {
        return personLastName;
    }

    public String gender()
    {
        return gender;
    }

    public Date birthday()
    {
        return birthday;
    }

    public Date creationDate()
    {
        return creationDate;
    }

    public String locationIp()
    {
        return locationIp;
    }

    public String browserUsed()
    {
        return browserUsed;
    }

    public long cityId()
    {
        return cityId;
    }

    public List<String> languages()
    {
        return languages;
    }

    public List<String> emails()
    {
        return emails;
    }

    public List<Long> tagIds()
    {
        return tagIds;
    }

    public List<Organization> studyAt()
    {
        return studyAt;
    }

    public List<Organization> workAt()
    {
        return workAt;
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
        if ( emails != null ? !ListUtils.listsEqual( sort( emails ), sort( that.emails ) ) : that.emails != null )
        { return false; }
        if ( gender != null ? !gender.equals( that.gender ) : that.gender != null )
        { return false; }
        if ( languages != null ? !ListUtils.listsEqual( sort( languages ), sort( that.languages ) )
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
        if ( tagIds != null ? !ListUtils.listsEqual( sort( tagIds ), sort( that.tagIds ) ) : that.tagIds != null )
        { return false; }
        if ( workAt != null ? !workAt.equals( that.workAt ) : that.workAt != null )
        { return false; }

        return true;
    }

    private <T extends Comparable> List<T> sort( List<T> list )
    {
        Collections.sort( list );
        return list;
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

    @Override
    public LdbcNoResult marshalResult( String serializedOperationResult )
    {
        return LdbcNoResult.INSTANCE;
    }

    @Override
    public String serializeResult( Object operationResultInstance ) throws SerializingMarshallingException
    {
        try
        {
            return objectMapper.writeValueAsString(
                    LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_NO_RESULT_DEFAULT_RESULT );
        }
        catch ( IOException e )
        {
            throw new SerializingMarshallingException( format( "Error while trying to serialize result\n%s",
                    operationResultInstance ), e );
        }
    }

    public static class Organization
    {
        private final long organizationId;
        private final int year;

        public Organization( long organizationId, int year )
        {
            this.organizationId = organizationId;
            this.year = year;
        }

        public long organizationId()
        {
            return organizationId;
        }

        public int year()
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
    public int type()
    {
        return TYPE;
    }
}
