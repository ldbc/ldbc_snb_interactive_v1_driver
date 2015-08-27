package com.ldbc.driver.csv.charseeker;

public class CharSeekerParams
{
    private final int bufferSize;
    private final char columnDelimiter;
    private final char arrayDelimiter;
    private final char tupleDelimiter;

    public CharSeekerParams( int bufferSize, char columnDelimiter, char arrayDelimiter, char tupleDelimiter )
    {
        this.bufferSize = bufferSize;
        this.columnDelimiter = columnDelimiter;
        this.arrayDelimiter = arrayDelimiter;
        this.tupleDelimiter = tupleDelimiter;
    }

    public int bufferSize()
    {
        return bufferSize;
    }

    public char columnDelimiter()
    {
        return columnDelimiter;
    }

    public char arrayDelimiter()
    {
        return arrayDelimiter;
    }

    public char tupleDelimiter()
    {
        return tupleDelimiter;
    }
}
