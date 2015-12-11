package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery14TopThreadInitiatorsResult
{
    private final long personId;
    private final String firstName;
    private final String lastName;
    private final int messageCount;
    private final int threadCount;

    public LdbcSnbBiQuery14TopThreadInitiatorsResult(
            long personId,
            String firstName,
            String lastName,
            int messageCount,
            int threadCount )
    {
        this.personId = personId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.messageCount = messageCount;
        this.threadCount = threadCount;
    }

    public long personId()
    {
        return personId;
    }

    public String firstName()
    {
        return firstName;
    }

    public String lastName()
    {
        return lastName;
    }

    public int messageCount()
    {
        return messageCount;
    }

    public int threadCount()
    {
        return threadCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery14TopThreadInitiatorsResult{" +
               "personId=" + personId +
               ", firstName='" + firstName + '\'' +
               ", lastName='" + lastName + '\'' +
               ", messageCount=" + messageCount +
               ", threadCount=" + threadCount +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery14TopThreadInitiatorsResult that = (LdbcSnbBiQuery14TopThreadInitiatorsResult) o;

        if ( personId != that.personId )
        { return false; }
        if ( messageCount != that.messageCount )
        { return false; }
        if ( threadCount != that.threadCount )
        { return false; }
        if ( firstName != null ? !firstName.equals( that.firstName ) : that.firstName != null )
        { return false; }
        return !(lastName != null ? !lastName.equals( that.lastName ) : that.lastName != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        result = 31 * result + messageCount;
        result = 31 * result + threadCount;
        return result;
    }
}
