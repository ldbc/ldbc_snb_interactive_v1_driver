package com.ldbc.driver.csv.simple;

import com.google.common.base.Charsets;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;

public class SimpleCsvFileWriter implements Closeable
{
    public static final String DEFAULT_COLUMN_SEPARATOR = "|";

    private final BufferedWriter bufferedWriter;
    private final String columnSeparator;

    public SimpleCsvFileWriter( File file, String columnSeparator ) throws IOException
    {
        this.bufferedWriter =
                new BufferedWriter( new OutputStreamWriter( new FileOutputStream( file ), Charsets.UTF_8 ) );

        this.columnSeparator = columnSeparator;
    }

    public void writeRows( Iterator<String[]> csvRows ) throws IOException
    {
        while ( csvRows.hasNext() )
        { writeRow( csvRows.next() ); }
    }

    public void writeRow( String... columns ) throws IOException
    {
        for ( int i = 0; i < columns.length - 1; i++ )
        {
            bufferedWriter.write( columns[i] );
            bufferedWriter.write( columnSeparator );
        }
        bufferedWriter.write( columns[columns.length - 1] );
        bufferedWriter.newLine();
    }

    @Override
    public void close() throws IOException
    {
        bufferedWriter.flush();
        bufferedWriter.close();
    }
}
