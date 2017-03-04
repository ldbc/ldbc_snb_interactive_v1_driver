package com.ldbc.driver.util;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

public class FileUtils
{
    public static void createOrFail( File file ) throws IOException
    {
        if ( !file.createNewFile() )
        {
            throw new IOException( "Failed to create results log" );
        }
    }

    public static String removeSuffix( String original, String suffix )
    {
        return (!original.contains( suffix )) ? original : original.substring( 0, original.lastIndexOf( suffix ) );
    }

    public static String removePrefix( String original, String prefix )
    {
        return (!original.contains( prefix )) ? original : original
                .substring( original.lastIndexOf( prefix ) + prefix.length(), original.length() );
    }

    public static List<File> filesWithSuffixInDirectory( File directory, final String fileNameSuffix )
    {
        return Lists.newArrayList(
                directory.listFiles(
                        new FilenameFilter()
                        {
                            @Override
                            public boolean accept( File dir, String name )
                            {
                                return name.endsWith( fileNameSuffix );
                            }
                        }
                )
        );
    }

    public static void assertDirectoryExists( File dir )
    {
        if ( !dir.exists() )
        {
            throw new RuntimeException( "Directory does not exist: " + dir.getAbsolutePath() );
        }
        else if ( !dir.isDirectory() )
        {
            throw new RuntimeException( "Is not a directory: " + dir.getAbsolutePath() );
        }
    }

    public static void assertFileExists( File file )
    {
        if ( !file.exists() )
        {
            throw new RuntimeException( "File does not exist: " + file.getAbsolutePath() );
        }
        else if ( file.isDirectory() )
        {
            throw new RuntimeException( "Is a directory, not a file: " + file.getAbsolutePath() );
        }
    }

    public static void forceRecreateFile( File file )
    {
        try
        {
            if ( file.exists() )
            {
                org.apache.commons.io.FileUtils.deleteQuietly( file );
            }
            if ( !file.createNewFile() )
            {
                throw new RuntimeException( "File already existed: " + file.getAbsolutePath() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to create file: " + file.getAbsolutePath(), e );
        }
    }

    public static void tryCreateDir( File dir, boolean failIfExists )
    {
        if ( dir.exists() && failIfExists )
        {
            throw new RuntimeException( "Directory already exists: " + dir.getAbsolutePath() );
        }
        else if ( !dir.exists() && !dir.mkdir() )
        {
            throw new RuntimeException( "Unable to create directory: " + dir.getAbsolutePath() );
        }
    }
}
