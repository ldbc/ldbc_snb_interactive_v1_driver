package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery8RelatedTopicsResult
{
    private final String tag;
    private final int count;

    public LdbcSnbBiQuery8RelatedTopicsResult( String tag, int count )
    {
        this.tag = tag;
        this.count = count;
    }

    public String tag()
    {
        return tag;
    }

    public int count()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery8Result{" +
               "tag='" + tag + '\'' +
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

        LdbcSnbBiQuery8RelatedTopicsResult that = (LdbcSnbBiQuery8RelatedTopicsResult) o;

        if ( count != that.count )
        { return false; }
        return !(tag != null ? !tag.equals( that.tag ) : that.tag != null);

    }

    @Override
    public int hashCode()
    {
        int result = tag != null ? tag.hashCode() : 0;
        result = 31 * result + count;
        return result;
    }
}
