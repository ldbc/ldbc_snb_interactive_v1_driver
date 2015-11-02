package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.util.List;

public class LdbcSnbBiQuery13PopularMonthlyTagsResult
{
    private final int year;
    private final int month;
    private final List<TagPopularity> tagPopularities;

    public LdbcSnbBiQuery13PopularMonthlyTagsResult( int year, int month, List<TagPopularity> tagPopularities )
    {
        this.year = year;
        this.month = month;
        this.tagPopularities = tagPopularities;
    }

    public int year()
    {
        return year;
    }

    public int month()
    {
        return month;
    }

    public List<TagPopularity> tagPopularities()
    {
        return tagPopularities;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery13PopularMonthlyTagsResult{" +
               "year=" + year +
               ", month=" + month +
               ", tagPopularities=" + tagPopularities +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery13PopularMonthlyTagsResult that = (LdbcSnbBiQuery13PopularMonthlyTagsResult) o;

        if ( year != that.year )
        { return false; }
        if ( month != that.month )
        { return false; }
        return !(tagPopularities != null ? !tagPopularities.equals( that.tagPopularities )
                                         : that.tagPopularities != null);

    }

    @Override
    public int hashCode()
    {
        int result = year;
        result = 31 * result + month;
        result = 31 * result + (tagPopularities != null ? tagPopularities.hashCode() : 0);
        return result;
    }

    public static class TagPopularity
    {
        private final String tag;
        private final int popularity;

        public TagPopularity( String tag, int popularity )
        {
            this.tag = tag;
            this.popularity = popularity;
        }

        public String tagName()
        {
            return tag;
        }

        public int popularity()
        {
            return popularity;
        }

        @Override
        public String toString()
        {
            return "TagPopularity{" +
                   "tag='" + tag + '\'' +
                   ", popularity=" + popularity +
                   '}';
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            TagPopularity that = (TagPopularity) o;

            if ( popularity != that.popularity )
            { return false; }
            return !(tag != null ? !tag.equals( that.tag ) : that.tag != null);

        }

        @Override
        public int hashCode()
        {
            int result = tag != null ? tag.hashCode() : 0;
            result = 31 * result + popularity;
            return result;
        }
    }
}
