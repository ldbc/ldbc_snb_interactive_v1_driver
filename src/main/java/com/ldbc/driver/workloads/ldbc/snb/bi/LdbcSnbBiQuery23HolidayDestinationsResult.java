package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery23HolidayDestinationsResult
{
    private final int messageCount;
    private final String country;
    private final int month;

    public LdbcSnbBiQuery23HolidayDestinationsResult( int messageCount, String country, int month )
    {
        this.messageCount = messageCount;
        this.country = country;
        this.month = month;
    }

    public int messageCount()
    {
        return messageCount;
    }

    public String country()
    {
        return country;
    }

    public int month()
    {
        return month;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery23HolidayDestinationsResult{" +
                "messageCount=" + messageCount +
                ", country='" + country + '\'' +
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
        return country != null ? country.equals(that.country) : that.country == null;
    }

    @Override
    public int hashCode() {
        int result = messageCount;
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + month;
        return result;
    }
}
