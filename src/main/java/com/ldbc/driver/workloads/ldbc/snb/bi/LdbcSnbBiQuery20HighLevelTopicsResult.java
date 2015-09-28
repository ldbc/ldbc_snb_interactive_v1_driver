package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery20HighLevelTopicsResult
{
    private final String tagClass;
    private final int count;

    public LdbcSnbBiQuery20HighLevelTopicsResult( String tagClass, int count )
    {
        this.tagClass = tagClass;
        this.count = count;
    }

    public String tagClass()
    {
        return tagClass;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery20Result{" +
               "tagClass='" + tagClass + '\'' +
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

        LdbcSnbBiQuery20HighLevelTopicsResult that = (LdbcSnbBiQuery20HighLevelTopicsResult) o;

        if ( count != that.count )
        { return false; }
        return !(tagClass != null ? !tagClass.equals( that.tagClass ) : that.tagClass != null);

    }

    @Override
    public int hashCode()
    {
        int result = tagClass != null ? tagClass.hashCode() : 0;
        result = 31 * result + count;
        return result;
    }
}
