package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery14TopThreadInitiatorsResult
{
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final int threadCount;
    private final int messageCount;

    public LdbcSnbBiQuery14TopThreadInitiatorsResult(
            long personId,
            String personFirstName,
            String personLastName,
            int threadCount,
            int messageCount)
    {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.threadCount = threadCount;
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

    public int threadCount()
    {
        return threadCount;
    }

    public int messageCount()
    {
        return messageCount;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery14TopThreadInitiatorsResult{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", threadCount=" + threadCount +
                ", messageCount=" + messageCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery14TopThreadInitiatorsResult that = (LdbcSnbBiQuery14TopThreadInitiatorsResult) o;

        if (personId != that.personId) return false;
        if (threadCount != that.threadCount) return false;
        if (messageCount != that.messageCount) return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null) return false;
        return personLastName != null ? personLastName.equals(that.personLastName) : that.personLastName == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + threadCount;
        result = 31 * result + messageCount;
        return result;
    }

}
