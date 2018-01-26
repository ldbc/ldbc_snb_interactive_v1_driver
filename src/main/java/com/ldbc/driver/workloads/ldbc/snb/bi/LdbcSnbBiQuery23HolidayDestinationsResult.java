package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery23HolidayDestinationsResult
{
    private final int messageCount;
    private final String destinationName;
    private final int month;

    public LdbcSnbBiQuery23HolidayDestinationsResult(int messageCount, String destinationName, int month )
    {
        this.messageCount = messageCount;
        this.destinationName = destinationName;
        this.month = month;
    }

    public int messageCount()
    {
        return messageCount;
    }

    public String destinationName()
    {
        return destinationName;
    }

    public int month()
    {
        return month;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery23HolidayDestinationsResult{" +
                "messageCount=" + messageCount +
                ", destinationName='" + destinationName + '\'' +
                ", month=" + month +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery23HolidayDestinationsResult that = (LdbcSnbBiQuery23HolidayDestinationsResult) o;

        if (messageCount != that.messageCount) return false;
        if (month != that.month) return false;
        return destinationName != null ? destinationName.equals(that.destinationName) : that.destinationName == null;
    }

    @Override
    public int hashCode() {
        int result = messageCount;
        result = 31 * result + (destinationName != null ? destinationName.hashCode() : 0);
        result = 31 * result + month;
        return result;
    }
}
