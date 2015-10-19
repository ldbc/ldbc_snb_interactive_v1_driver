package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery1PostingSummaryResult
{
    public static final int CATEGORY_SHORT = 0;
    public static final int CATEGORY_ONE_LINER = 1;
    public static final int CATEGORY_TWEET = 2;
    public static final int CATEGORY_LONG = 3;

    private final int year;
    private final boolean isComment;
    private final int messageLengthCategory;
    private final long messageCount;
    private final long messageLengthMean;
    private final long messageLengthSum;
    private final double percentOfTotalMessageCount;

    public LdbcSnbBiQuery1PostingSummaryResult(
            int year,
            boolean isComment,
            int messageLengthCategory,
            long messageCount,
            long messageLengthMean,
            long messageLengthSum,
            double percentOfTotalMessageCount )
    {
        this.year = year;
        this.isComment = isComment;
        this.messageLengthCategory = messageLengthCategory;
        this.messageCount = messageCount;
        this.messageLengthMean = messageLengthMean;
        this.messageLengthSum = messageLengthSum;
        this.percentOfTotalMessageCount = percentOfTotalMessageCount;
    }

    public int year()
    {
        return year;
    }

    public boolean isComment()
    {
        return isComment;
    }

    public int messageLengthCategory()
    {
        return messageLengthCategory;
    }

    public long messageCount()
    {
        return messageCount;
    }

    public long messageLengthMean()
    {
        return messageLengthMean;
    }

    public long messageLengthSum()
    {
        return messageLengthSum;
    }

    public double percentOfTotalMessageCount()
    {
        return percentOfTotalMessageCount;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery1Result{" +
               "year=" + year +
               ", isComment=" + isComment +
               ", messageLengthCategory=" + messageLengthCategory +
               ", messageCount=" + messageCount +
               ", messageLengthMean=" + messageLengthMean +
               ", messageLengthSum=" + messageLengthSum +
               ", percentOfTotalMessageCount=" + percentOfTotalMessageCount +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery1PostingSummaryResult result = (LdbcSnbBiQuery1PostingSummaryResult) o;

        if ( year != result.year )
        { return false; }
        if ( isComment != result.isComment )
        { return false; }
        if ( messageLengthCategory != result.messageLengthCategory )
        { return false; }
        if ( messageCount != result.messageCount )
        { return false; }
        if ( messageLengthMean != result.messageLengthMean )
        { return false; }
        if ( messageLengthSum != result.messageLengthSum )
        { return false; }
        return Double.compare( result.percentOfTotalMessageCount, percentOfTotalMessageCount ) == 0;

    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = year;
        result = 31 * result + (isComment ? 1 : 0);
        result = 31 * result + messageLengthCategory;
        result = 31 * result + (int) (messageCount ^ (messageCount >>> 32));
        result = 31 * result + (int) (messageLengthMean ^ (messageLengthMean >>> 32));
        result = 31 * result + (int) (messageLengthSum ^ (messageLengthSum >>> 32));
        temp = Double.doubleToLongBits( percentOfTotalMessageCount );
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
