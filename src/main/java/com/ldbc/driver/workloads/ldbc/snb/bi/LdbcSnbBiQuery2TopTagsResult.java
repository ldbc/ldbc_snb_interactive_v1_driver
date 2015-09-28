package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery2TopTagsResult
{
    private final String country;
    private final int month;
    private final String gender;
    private final int ageGroup;
    private final String tag;
    private final int count;

    public LdbcSnbBiQuery2TopTagsResult(
            String country,
            int month,
            String gender,
            int ageGroup,
            String tag,
            int count )
    {
        this.country = country;
        this.month = month;
        this.gender = gender;
        this.ageGroup = ageGroup;
        this.tag = tag;
        this.count = count;
    }

    public String country()
    {
        return country;
    }

    public int month()
    {
        return month;
    }

    public String gender()
    {
        return gender;
    }

    public int ageGroup()
    {
        return ageGroup;
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
        return "LdbcSnbBiQuery2TopTagsResult{" +
               "country='" + country + '\'' +
               ", month=" + month +
               ", gender='" + gender + '\'' +
               ", ageGroup=" + ageGroup +
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

        LdbcSnbBiQuery2TopTagsResult that = (LdbcSnbBiQuery2TopTagsResult) o;

        if ( month != that.month )
        { return false; }
        if ( ageGroup != that.ageGroup )
        { return false; }
        if ( count != that.count )
        { return false; }
        if ( country != null ? !country.equals( that.country ) : that.country != null )
        { return false; }
        if ( gender != null ? !gender.equals( that.gender ) : that.gender != null )
        { return false; }
        return !(tag != null ? !tag.equals( that.tag ) : that.tag != null);

    }

    @Override
    public int hashCode()
    {
        int result = country != null ? country.hashCode() : 0;
        result = 31 * result + month;
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + ageGroup;
        result = 31 * result + (tag != null ? tag.hashCode() : 0);
        result = 31 * result + count;
        return result;
    }
}
