package com.ldbc.driver.workloads.ldbc.snb.bi;

public class LdbcSnbBiQuery1PostingSummaryResult
{
    public static final int CATEGORY_SHORT = 0;
    public static final int CATEGORY_ONE_LINER = 1;
    public static final int CATEGORY_TWEET = 2;
    public static final int CATEGORY_LONG = 3;

    private final int messageYear;
    private final boolean isComment;
    private final int lengthCategory;
    private final long messageCount;
    private final long averageMessageLength;
    private final long sumMessageLength;
    private final float percentOfMessages;

    public LdbcSnbBiQuery1PostingSummaryResult(
            int messageYear,
            boolean isComment,
            int lengthCategory,
            long messageCount,
            long averageMessageLength,
            long sumMessageLength,
            float percentOfMessages)
    {
        this.messageYear = messageYear;
        this.isComment = isComment;
        this.lengthCategory = lengthCategory;
        this.messageCount = messageCount;
        this.averageMessageLength = averageMessageLength;
        this.sumMessageLength = sumMessageLength;
        this.percentOfMessages = percentOfMessages;
    }

    public int messageYear()
    {
        return messageYear;
    }

    public boolean isComment()
    {
        return isComment;
    }

    public int lengthCategory()
    {
        return lengthCategory;
    }

    public long messageCount()
    {
        return messageCount;
    }

    public long averageMessageLength()
    {
        return averageMessageLength;
    }

    public long sumMessageLength()
    {
        return sumMessageLength;
    }

    public float percentOfMessages()
    {
        return percentOfMessages;
    }

    @Override
    public String toString()
    {
        return "LdbcSnbBiQuery1PostingSummaryResult{" +
               "messageYear=" + messageYear +
               ", isComment=" + isComment +
               ", lengthCategory=" + lengthCategory +
               ", messageCount=" + messageCount +
               ", averageMessageLength=" + averageMessageLength +
               ", sumMessageLength=" + sumMessageLength +
               ", percentOfMessages=" + percentOfMessages +
               '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        LdbcSnbBiQuery1PostingSummaryResult that = (LdbcSnbBiQuery1PostingSummaryResult) o;

        if ( messageYear != that.messageYear)
        { return false; }
        if ( isComment != that.isComment )
        { return false; }
        if ( lengthCategory != that.lengthCategory)
        { return false; }
        if ( messageCount != that.messageCount )
        { return false; }
        if ( averageMessageLength != that.averageMessageLength)
        { return false; }
        if ( sumMessageLength != that.sumMessageLength)
        { return false; }
        return floatEpsilonCompare( that.percentOfMessages, percentOfMessages, EPSILON );
    }

    @Override
    public int hashCode()
    {
        int result = messageYear;
        result = 31 * result + (isComment ? 1 : 0);
        result = 31 * result + lengthCategory;
        result = 31 * result + (int) (messageCount ^ (messageCount >>> 32));
        result = 31 * result + (int) (averageMessageLength ^ (averageMessageLength >>> 32));
        result = 31 * result + (int) (sumMessageLength ^ (sumMessageLength >>> 32));
        result = 31 * result +
                 (percentOfMessages != +0.0f ? Float.floatToIntBits(percentOfMessages) : 0);
        return result;
    }

    private static final float EPSILON = 0.00001f;

    private boolean floatEpsilonCompare( float a, float b, float epsilon )
    {
        return Math.abs( a - b ) <= epsilon;
    }
}
