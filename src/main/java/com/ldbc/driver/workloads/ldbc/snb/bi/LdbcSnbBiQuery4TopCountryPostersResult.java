package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery4TopCountryPostersResult
{
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long personCreationDate;
    private final int messageCount;

    public LdbcSnbBiQuery4TopCountryPostersResult(
            long personId,
            String personFirstName,
            String personLastName,
            long personCreationDate,
            int messageCount)
    {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.personCreationDate = personCreationDate;
        this.messageCount = messageCount;
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

    public int messageCount()
    {
        return messageCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery4TopCountryPostersResult{" +
               "personId=" + personId +
               ", personFirstName='" + personFirstName + '\'' +
               ", personLastName='" + personLastName + '\'' +
               ", personCreationDate=" + personCreationDate +
               ", messageCount=" + messageCount +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery4TopCountryPostersResult that = (LdbcSnbBiQuery4TopCountryPostersResult) o;

        if ( personId != that.personId )
        { return false; }
        if ( personCreationDate != that.personCreationDate)
        { return false; }
        if ( messageCount != that.messageCount)
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
        result = 31 * result + messageCount;
        return result;
    }
}
