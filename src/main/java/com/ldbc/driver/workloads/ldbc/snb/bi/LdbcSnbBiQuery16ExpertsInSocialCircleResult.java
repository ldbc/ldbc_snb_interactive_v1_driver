package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery16ExpertsInSocialCircleResult
{
    private final long personId;
    private final String tagName;
    private final int messageCount;

    public LdbcSnbBiQuery16ExpertsInSocialCircleResult(long personId, String tagName, int messageCount)
    {
        this.personId = personId;
        this.tagName = tagName;
        this.messageCount = messageCount;
    }

    public long personId()
    {
        return personId;
    }

    public String tag()
    {
        return tagName;
    }

    public int count()
    {
        return messageCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery16ExpertsInSocialCircleResult{" +
               "personId=" + personId +
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

        LdbcSnbBiQuery16ExpertsInSocialCircleResult that = (LdbcSnbBiQuery16ExpertsInSocialCircleResult) o;

        if ( personId != that.personId )
        { return false; }
        if ( messageCount != that.messageCount)
        { return false; }
        return !(tagName != null ? !tagName.equals( that.tagName) : that.tagName != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (tagName != null ? tagName.hashCode() : 0);
        result = 31 * result + messageCount;
        return result;
    }
}
