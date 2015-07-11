package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery16Result
{
    private final long personId;
    private final String tag;
    private final int count;

    public LdbcSnbBiQuery16Result( long personId, String tag, int count )
    {
        this.personId = personId;
        this.tag = tag;
        this.count = count;
    }

    public long personId()
    {
        return personId;
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
        return "LdbcSnbBiQuery16Result{" +
               "personId=" + personId +
               ", tag='" + tag + '\'' +
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

        LdbcSnbBiQuery16Result that = (LdbcSnbBiQuery16Result) o;

        if ( personId != that.personId )
        { return false; }
        if ( count != that.count )
        { return false; }
        return !(tag != null ? !tag.equals( that.tag ) : that.tag != null);

    }

    @Override
    public int hashCode()
    {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (tag != null ? tag.hashCode() : 0);
        result = 31 * result + count;
        return result;
    }
}
