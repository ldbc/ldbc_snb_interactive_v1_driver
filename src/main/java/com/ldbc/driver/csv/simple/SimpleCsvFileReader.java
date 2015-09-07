package com.ldbc.driver.csv.simple;

import com.google.common.base.Charsets;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class SimpleCsvFileReader implements Iterator<String[]>, Closeable
{
    public static final String DEFAULT_COLUMN_SEPARATOR_REGEX_STRING = "\\|";
    private final Pattern columnSeparatorPattern;
    private final BufferedReader csvReader;

    private String[] next = null;
    private boolean closed = false;

    public SimpleCsvFileReader( File csvFile, String separatorRegexString ) throws FileNotFoundException
    {
        this(
                new BufferedReader( new InputStreamReader( new FileInputStream( csvFile ), Charsets.UTF_8 ) ),
                Pattern.compile( separatorRegexString )
        );
    }

    public SimpleCsvFileReader( BufferedReader reader, String separatorRegexString ) throws FileNotFoundException
    {
        this(
                reader,
                Pattern.compile( separatorRegexString )
        );
    }

    private SimpleCsvFileReader( BufferedReader reader, Pattern separatorRegexPattern ) throws FileNotFoundException
    {
        this.csvReader = reader;
        this.columnSeparatorPattern = separatorRegexPattern;
    }

    @Override
    public boolean hasNext()
    {
        if ( closed )
        { return false; }
        next = (next == null) ? nextLine() : next;
        if ( null == next )
        {
            return false;
        }
        return (null != next);
    }

    @Override
    public String[] next()
    {
        next = (null == next) ? nextLine() : next;
        if ( null == next )
        { throw new NoSuchElementException( "No more lines to read" ); }
        String[] tempNext = next;
        next = null;
        return tempNext;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private String[] nextLine()
    {
        String csvLine;
        try
        {
            csvLine = csvReader.readLine();
            if ( null == csvLine )
            { return null; }
            return parseLine( csvLine );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( format( "Error retrieving next csv entry from file" ), e );
        }
    }

    private String[] parseLine( String csvLine )
    {
        return columnSeparatorPattern.split( csvLine, -1 );
    }

    @Override
    public void close()
    {
        if ( closed )
        {
            return;
            // TODO this really should throw an exception
//            String errMsg = "Can not close file multiple times";
//            throw new RuntimeException(errMsg);
        }
        if ( null == csvReader )
        {
            throw new RuntimeException( "Can not close file - reader is null" );
        }
        try
        {
            csvReader.close();
        }
        catch ( IOException e )
        {
            String errMsg = format( "Error closing file [%s]", csvReader );
            throw new RuntimeException( errMsg, e );
        }
    }
}
