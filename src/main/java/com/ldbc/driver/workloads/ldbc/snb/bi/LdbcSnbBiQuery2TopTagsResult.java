package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery2TopTagsResult
{
    private final String countryName;
    private final int messageMonth;
    private final String personGender;
    private final int ageGroup;
    private final String tagName;
    private final int messageCount;

    public LdbcSnbBiQuery2TopTagsResult(
            String countryName,
            int messageMonth,
            String personGender,
            int ageGroup,
            String tagName,
            int messageCount)
    {
        this.countryName = countryName;
        this.messageMonth = messageMonth;
        this.personGender = personGender;
        this.ageGroup = ageGroup;
        this.tagName = tagName;
        this.messageCount = messageCount;
    }

    public String countryName()
    {
        return countryName;
    }

    public int messageMonth()
    {
        return messageMonth;
    }

    public String personGender()
    {
        return personGender;
    }

    public int ageGroup()
    {
        return ageGroup;
    }

    public String tagName()
    {
        return tagName;
    }

    public int messageCount()
    {
        return messageCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery2TopTagsResult{" +
               "countryName='" + countryName + '\'' +
               ", messageMonth=" + messageMonth +
               ", personGender='" + personGender + '\'' +
               ", ageGroup=" + ageGroup +
               ", tagName='" + tagName + '\'' +
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

        LdbcSnbBiQuery2TopTagsResult that = (LdbcSnbBiQuery2TopTagsResult) o;

        if ( messageMonth != that.messageMonth)
        { return false; }
        if ( ageGroup != that.ageGroup )
        { return false; }
        if ( messageCount != that.messageCount)
        { return false; }
        if ( countryName != null ? !countryName.equals( that.countryName) : that.countryName != null )
        { return false; }
        if ( personGender != null ? !personGender.equals( that.personGender) : that.personGender != null )
        { return false; }
        return !(tagName != null ? !tagName.equals( that.tagName) : that.tagName != null);

    }

    @Override
    public int hashCode()
    {
        int result = countryName != null ? countryName.hashCode() : 0;
        result = 31 * result + messageMonth;
        result = 31 * result + (personGender != null ? personGender.hashCode() : 0);
        result = 31 * result + ageGroup;
        result = 31 * result + (tagName != null ? tagName.hashCode() : 0);
        result = 31 * result + messageCount;
        return result;
    }
}
