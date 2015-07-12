package com.ldbc.driver.util;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FilenameFilter;
import java.util.List;

public class FileUtils
{
    public static String removeSuffix( String original, String suffix )
    {
        return (original.indexOf( suffix ) == -1) ? original : original.substring( 0, original.lastIndexOf( suffix ) );
    }

    public static String removePrefix( String original, String prefix )
    {
        return (original.indexOf( prefix ) == -1) ? original : original
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
}
