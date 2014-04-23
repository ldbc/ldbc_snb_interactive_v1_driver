package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Arrays;
import java.util.Date;

public class LdbcUpdate1AddPerson extends Operation<Object> {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final String gender;
    private final Date birthday; // input format "1984-03-22"
    private final Date creationDate; // input format "2004-03-22"
    private final String locationIp;
    private final String browserUsed;
    private final long cityId;
    private final String[] languages;
    private final String[] emails;
    private final long[] tagIds;
    private final Organization[] studyAt;
    private final Organization[] workAt;

    public LdbcUpdate1AddPerson(long personId,
                                String personFirstName,
                                String personLastName,
                                String gender,
                                Date birthday,
                                Date creationDate,
                                String locationIp,
                                String browserUsed,
                                long cityId,
                                String[] languages,
                                String[] emails,
                                long[] tagIds,
                                Organization[] studyAt,
                                Organization[] workAt) {
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

    public long personId() {
        return personId;
    }

    public String personFirstName() {
        return personFirstName;
    }

    public String personLastName() {
        return personLastName;
    }

    public String gender() {
        return gender;
    }

    public Date birthday() {
        return birthday;
    }

    public Date creationDate() {
        return creationDate;
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

    public String[] languages() {
        return languages;
    }

    public String[] emails() {
        return emails;
    }

    public long[] tagIds() {
        return tagIds;
    }

    public Organization[] studyAt() {
        return studyAt;
    }

    public Organization[] workAt() {
        return workAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcUpdate1AddPerson that = (LdbcUpdate1AddPerson) o;

        if (cityId != that.cityId) return false;
        if (personId != that.personId) return false;
        if (birthday != null ? !birthday.equals(that.birthday) : that.birthday != null) return false;
        if (browserUsed != null ? !browserUsed.equals(that.browserUsed) : that.browserUsed != null) return false;
        if (creationDate != null ? !creationDate.equals(that.creationDate) : that.creationDate != null) return false;
        if (!Arrays.equals(emails, that.emails)) return false;
        if (gender != null ? !gender.equals(that.gender) : that.gender != null) return false;
        if (!Arrays.equals(languages, that.languages)) return false;
        if (locationIp != null ? !locationIp.equals(that.locationIp) : that.locationIp != null) return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;
        if (!Arrays.equals(studyAt, that.studyAt)) return false;
        if (!Arrays.equals(tagIds, that.tagIds)) return false;
        if (!Arrays.equals(workAt, that.workAt)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + (birthday != null ? birthday.hashCode() : 0);
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        result = 31 * result + (locationIp != null ? locationIp.hashCode() : 0);
        result = 31 * result + (browserUsed != null ? browserUsed.hashCode() : 0);
        result = 31 * result + (int) (cityId ^ (cityId >>> 32));
        result = 31 * result + (languages != null ? Arrays.hashCode(languages) : 0);
        result = 31 * result + (emails != null ? Arrays.hashCode(emails) : 0);
        result = 31 * result + (tagIds != null ? Arrays.hashCode(tagIds) : 0);
        result = 31 * result + (studyAt != null ? Arrays.hashCode(studyAt) : 0);
        result = 31 * result + (workAt != null ? Arrays.hashCode(workAt) : 0);
        return result;
    }

    @Override
    public String toString() {
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
                ", languages=" + Arrays.toString(languages) +
                ", emails=" + Arrays.toString(emails) +
                ", tagIds=" + Arrays.toString(tagIds) +
                ", studyAt=" + Arrays.toString(studyAt) +
                ", workAt=" + Arrays.toString(workAt) +
                '}';
    }

    public static class Organization {
        private final long organizationId;
        private final int year;

        public Organization(long organizationId, int year) {
            this.organizationId = organizationId;
            this.year = year;
        }

        public long organizationId() {
            return organizationId;
        }

        public int year() {
            return year;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Organization that = (Organization) o;

            if (organizationId != that.organizationId) return false;
            if (year != that.year) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (organizationId ^ (organizationId >>> 32));
            result = 31 * result + year;
            return result;
        }

        @Override
        public String toString() {
            return "Organization{" +
                    "organizationId=" + organizationId +
                    ", year=" + year +
                    '}';
        }
    }
}
