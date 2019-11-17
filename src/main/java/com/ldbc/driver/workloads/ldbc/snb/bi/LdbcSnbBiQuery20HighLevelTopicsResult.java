package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery20HighLevelTopicsResult
{
    private final String tagClassName;
    private final int messageCount;

    public LdbcSnbBiQuery20HighLevelTopicsResult(String tagClassName, int messageCount)
    {
        this.tagClassName = tagClassName;
        this.messageCount = messageCount;
    }

    public String tagClassName()
    {
        return tagClassName;
    }

    public int messageCount()
    {
        return messageCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery20HighLevelTopicsResult{" +
               "tagClassName='" + tagClassName + '\'' +
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

        LdbcSnbBiQuery20HighLevelTopicsResult that = (LdbcSnbBiQuery20HighLevelTopicsResult) o;

        if ( messageCount != that.messageCount)
        { return false; }
        return !(tagClassName != null ? !tagClassName.equals( that.tagClassName) : that.tagClassName != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagClassName != null ? tagClassName.hashCode() : 0;
        result = 31 * result + messageCount;
        return result;
    }
}
