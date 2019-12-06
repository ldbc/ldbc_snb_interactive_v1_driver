package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery5TopCountryPostersResult
{
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long personCreationDate;
    private final int postCount;

    public LdbcSnbBiQuery5TopCountryPostersResult(
            long personId,
            String personFirstName,
            String personLastName,
            long personCreationDate,
            int postCount)
    {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.personCreationDate = personCreationDate;
        this.postCount = postCount;
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

    public long personCreationDate()
    {
        return personCreationDate;
    }

    public int postCount()
    {
        return postCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery5TopCountryPostersResult{" +
               "personId=" + personId +
               ", personFirstName='" + personFirstName + '\'' +
               ", personLastName='" + personLastName + '\'' +
               ", personCreationDate=" + personCreationDate +
               ", postCount=" + postCount +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery5TopCountryPostersResult that = (LdbcSnbBiQuery5TopCountryPostersResult) o;

        if ( personId != that.personId )
        { return false; }
        if ( personCreationDate != that.personCreationDate)
        { return false; }
        if ( postCount != that.postCount)
        { return false; }
        if ( personFirstName != null ? !personFirstName.equals( that.personFirstName) : that.personFirstName != null )
        { return false; }
        return !(personLastName != null ? !personLastName.equals( that.personLastName) : that.personLastName != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (int) (personCreationDate ^ (personCreationDate >>> 32));
        result = 31 * result + postCount;
        return result;
    }
}
