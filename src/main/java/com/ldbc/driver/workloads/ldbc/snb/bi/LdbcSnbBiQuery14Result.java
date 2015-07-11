package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery14Result
{
    private final long personId;
    private final String firstName;
    private final String lastName;
    private final int count;
    private final int threadCount;

    public LdbcSnbBiQuery14Result( long personId, String firstName, String lastName, int count, int threadCount )
    {
        this.personId = personId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.count = count;
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

    public int count()
    {
        return count;
    }

    public int threadCount()
    {
        return threadCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery14Result{" +
               "personId=" + personId +
               ", firstName='" + firstName + '\'' +
               ", lastName='" + lastName + '\'' +
               ", count=" + count +
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

        LdbcSnbBiQuery14Result that = (LdbcSnbBiQuery14Result) o;

        if ( personId != that.personId )
        { return false; }
        if ( count != that.count )
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
        result = 31 * result + count;
        result = 31 * result + threadCount;
        return result;
    }
}
