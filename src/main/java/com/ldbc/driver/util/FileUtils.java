package com.ldbc.driver.util;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

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
        return Lists.newArrayList( directory.listFiles( ( dir, name ) -> name.endsWith( fileNameSuffix ) ) );
    }

    public static void assertFileDoesNotExist( File file )
    {
        if ( file.exists() )
        {
            throw new RuntimeException( "File already exists: " + file.getAbsolutePath() );
        }
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

    public static void tryCreateFile( File file, boolean failIfExists )
    {
        try
        {
            if ( file.exists() )
            {
                if ( failIfExists )
                {
                    throw new RuntimeException( "File already exists: " + file.getAbsolutePath() );
                }
                else if ( file.isDirectory() )
                {
                    throw new RuntimeException( "Is a directory: " + file.getAbsolutePath() );
                }
            }
            else if ( !file.createNewFile() )
            {
                throw new RuntimeException( "Unable to create file: " + file.getAbsolutePath() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to create file: " + file.getAbsolutePath(), e );
        }
    }

    public static void tryCreateDirs( File dir, boolean failIfExists )
    {
        if ( dir.exists() )
        {
            if ( failIfExists )
            {
                throw new RuntimeException( "Directory already exists: " + dir.getAbsolutePath() );
            }
            else if ( !dir.isDirectory() )
            {
                throw new RuntimeException( "Is not a directory: " + dir.getAbsolutePath() );
            }
        }
        else if ( !dir.mkdirs() )
        {
            throw new RuntimeException( "Unable to create directory structure: " + dir.getAbsolutePath() );
        }
    }

    public static void copyDir( File from, File to )
    {
        try
        {
            System.out.println( format( "Copying directory...\n" +
                            "From:     %s\n" +
                            "To:       %s",
                    from.getAbsolutePath(),
                    to.getAbsolutePath() ) );
            FileUtils.assertDirectoryExists( from );
            FileUtils.assertFileDoesNotExist( to );
            // Alternative method of copying database into working directory
            org.apache.commons.io.FileUtils.copyDirectory( from, to );
            FileUtils.assertDirectoryExists( from );
            FileUtils.assertDirectoryExists( to );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error copying directory", e );
        }
    }

    public static long bytes( Path dir )
    {
        if ( !Files.exists( dir ) )
        {
            return 0;
        }
        try ( Stream<Path> paths = Files.list( dir ) )
        {
            long bytes = 0;
            for ( Path path : paths.collect( toList() ) )
            {
                if ( path.toFile().isFile() )
                {
                    bytes += path.toFile().length();
                }
                else
                {
                    bytes += bytes( path );
                }
            }
            return bytes;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static void printlnsToFile( File file, Instant timeStamp, String... lines )
    {
        try ( PrintWriter printer = new PrintWriter( new FileOutputStream( file, true ) ) )
        {
            Arrays.stream( lines ).forEach( line -> printer.println( "[" + timeStamp + "] " + line ) );
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( "Error writing line to file\n" +
                    "Line: " + lines + "\n" +
                    "File: " + file.getAbsolutePath(), e );
        }
    }

    public static String inputStringToString( InputStream is ) throws IOException
    {
        try ( BufferedReader buffer = new BufferedReader( new InputStreamReader( is ) ) )
        {
            return buffer.lines().collect( joining( "\n" ) );
        }
    }
}
