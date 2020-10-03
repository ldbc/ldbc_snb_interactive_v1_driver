package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery7RelatedTopicsResult
{
    private final String relatedTagName;
    private final int count;

    public LdbcSnbBiQuery7RelatedTopicsResult(String relatedTagName, int count )
    {
        this.relatedTagName = relatedTagName;
        this.count = count;
    }

    public String tag()
    {
        return relatedTagName;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery7RelatedTopicsResult{" +
               "relatedTagName='" + relatedTagName + '\'' +
               ", count=" + count +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery7RelatedTopicsResult that = (LdbcSnbBiQuery7RelatedTopicsResult) o;

        if ( count != that.count )
        { return false; }
        return !(relatedTagName != null ? !relatedTagName.equals( that.relatedTagName) : that.relatedTagName != null);

    }

    @Override
    public int hashCode()
    {
        int result = relatedTagName != null ? relatedTagName.hashCode() : 0;
        result = 31 * result + count;
        return result;
    }
}
