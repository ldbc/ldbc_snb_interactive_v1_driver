package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery1Result
{
    private final int year;
    private final boolean isReply;
    private final int category;
    private final int count;
    private final int averageLength;
    private final int total;
    private final double percent;

    public LdbcSnbBiQuery1Result(
            int year,
            boolean isReply,
            int category,
            int count,
            int averageLength,
            int total,
            double percent )
    {
        this.year = year;
        this.isReply = isReply;
        this.category = category;
        this.count = count;
        this.averageLength = averageLength;
        this.total = total;
        this.percent = percent;
    }

    public int year()
    {
        return year;
    }

    public boolean isReply()
    {
        return isReply;
    }

    public int category()
    {
        return category;
    }

    public int count()
    {
        return count;
    }

    public int averageLength()
    {
        return averageLength;
    }

    public int total()
    {
        return total;
    }

    public double percent()
    {
        return percent;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery1Result{" +
               "year=" + year +
               ", isReply=" + isReply +
               ", category=" + category +
               ", count=" + count +
               ", averageLength=" + averageLength +
               ", total=" + total +
               ", percent=" + percent +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery1Result that = (LdbcSnbBiQuery1Result) o;

        if ( year != that.year )
        { return false; }
        if ( isReply != that.isReply )
        { return false; }
        if ( category != that.category )
        { return false; }
        if ( count != that.count )
        { return false; }
        if ( averageLength != that.averageLength )
        { return false; }
        if ( total != that.total )
        { return false; }
        return Double.compare( that.percent, percent ) == 0;

    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = year;
        result = 31 * result + (isReply ? 1 : 0);
        result = 31 * result + category;
        result = 31 * result + count;
        result = 31 * result + averageLength;
        result = 31 * result + total;
        temp = Double.doubleToLongBits( percent );
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
