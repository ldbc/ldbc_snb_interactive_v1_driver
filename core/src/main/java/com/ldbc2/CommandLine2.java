/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.ldbc2;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Vector;

import com.ldbc.data.ByteIterator;
import com.ldbc.data.StringByteIterator;
import com.ldbc.db2.Db2;
import com.ldbc.db2.DbException2;
import com.ldbc.util.MapUtils;
// TODO remove
import com.ldbc.workloads.ycsb.CoreWorkloadProperties;

/**
 * A simple command line client to a database, using the appropriate
 * com.yahoo.ycsb.DB implementation.
 */
public class CommandLine2
{
    private static final String DEFAULT_DB = "com.yahoo.ycsb.BasicDB";

    private static void printUsageMessage( PrintStream printStream )
    {
        printStream.println( "YCSB Command Line Client" );
        printStream.println( "Usage: java com.yahoo.ycsb.CommandLine [options]" );
        printStream.println( "Options:" );
        printStream.println( "  -P filename: Specify a property file" );
        printStream.println( "  -p name=value: Specify a property value" );
        printStream.println( "  -db classname: Use a specified DB class (can also set the \"db\" property)" );
        printStream.println( "  -table tablename: Use the table name instead of the default \""
                             + CoreWorkloadProperties.TABLENAME_DEFAULT + "\"" );
        printStream.println();
    }

    private static void printHelpMessage( PrintStream printStream )
    {
        printStream.println( "Commands:" );
        printStream.println( "  read key [field1 field2 ...] - Read a record" );
        printStream.println( "  scan key recordcount [field1 field2 ...] - Scan starting at key" );
        printStream.println( "  insert key name1=value1 [name2=value2 ...] - Insert a new record" );
        printStream.println( "  update key name1=value1 [name2=value2 ...] - Update a record" );
        printStream.println( "  delete key - Delete a record" );
        printStream.println( "  table [tablename] - Get or [set] the name of the table" );
        printStream.println( "  quit - Quit" );
    }

    private static void printIntroductionMessage( PrintStream printStream )
    {
        printStream.println( "YCSB Command Line client" );
        printStream.println( "Type \"help\" for command line help" );
        printStream.println( "Start with \"-help\" for usage info" );
    }

    public static void main( String[] args ) throws ClientException2
    {
        int argIndex = 0;

        Map<String, String> commandlineProperties = new HashMap<String, String>();
        Properties fileProperties = new Properties();
        String argTableName = CoreWorkloadProperties.TABLENAME_DEFAULT;

        while ( ( argIndex < args.length ) && ( args[argIndex].startsWith( "-" ) ) )
        {
            if ( ( args[argIndex].equals( "-help" ) ) || ( args[argIndex].equals( "--help" ) )
                 || ( args[argIndex].equals( "-?" ) ) || ( args[argIndex].equals( "--?" ) ) )
            {
                printUsageMessage( System.out );
                System.exit( 0 );
            }

            if ( args[argIndex].equals( "-db" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    printUsageMessage( System.out );
                    System.exit( 0 );
                }
                String argDb = args[argIndex];
                commandlineProperties.put( "db", argDb );
                argIndex++;
            }
            else if ( args[argIndex].equals( "-P" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    printUsageMessage( System.out );
                    System.exit( 0 );
                }
                String argPropertyFile = args[argIndex];
                argIndex++;

                // TODO remove now
                // Properties myfileprops = new Properties();
                // try
                // {
                // myfileprops.load( new FileInputStream( propfile ) );
                // }
                // catch ( IOException e )
                // {
                // System.out.println( e.getMessage() );
                // System.exit( 0 );
                // }
                //
                // for ( Enumeration e = myfileprops.propertyNames();
                // e.hasMoreElements(); )
                // {
                // String prop = (String) e.nextElement();
                //
                // fileProperties.setProperty( prop, myfileprops.getProperty(
                // prop ) );
                // }
                try
                {
                    fileProperties.load( new FileInputStream( argPropertyFile ) );
                }
                catch ( Exception e )
                {
                    throw new ClientException2( "Error encountered loading properties from file", e.getCause() );
                }

            }
            else if ( args[argIndex].equals( "-p" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    printUsageMessage( System.out );
                    System.exit( 0 );
                }
                int equalsCharPosition = args[argIndex].indexOf( '=' );
                if ( equalsCharPosition < 0 )
                {
                    printUsageMessage( System.out );
                    System.exit( 0 );
                }

                String argPropertyName = args[argIndex].substring( 0, equalsCharPosition );
                String argPropertyValue = args[argIndex].substring( equalsCharPosition + 1 );
                commandlineProperties.put( argPropertyName, argPropertyValue );
                argIndex++;
            }
            else if ( args[argIndex].equals( "-table" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    printUsageMessage( System.out );
                    System.exit( 0 );
                }
                argTableName = args[argIndex];
                argIndex++;
            }
            else
            {
                System.out.println( "Unknown option " + args[argIndex] );
                printUsageMessage( System.out );
                System.exit( 0 );
            }

            if ( argIndex >= args.length )
            {
                break;
            }
        }

        if ( argIndex != args.length )
        {
            printUsageMessage( System.out );
            System.exit( 0 );
        }

        commandlineProperties = MapUtils.mergePropertiesToMap( fileProperties, commandlineProperties, false );

        printIntroductionMessage( System.out );

        Db2 db = null;
        try
        {
            // TODO remove now
            // String dbname = commandlineProperties.getProperty( "db",
            // DEFAULT_DB
            // );
            String dbName = MapUtils.mapGetDefault( commandlineProperties, "db", DEFAULT_DB );
            ClassLoader classLoader = CommandLine2.class.getClassLoader();
            Class<? extends Db2> dbClass = (Class<? extends Db2>) classLoader.loadClass( dbName );
            db = dbClass.newInstance();
        }
        catch ( Exception e )
        {
            throw new ClientException2( "Error encountered creating db from dynamically loaded class", e.getCause() );
        }

        try
        {
            db.init( commandlineProperties );
        }
        catch ( DbException2 e )
        {
            throw new ClientException2( "Error encountered initializing db", e.getCause() );
        }

        System.out.println( "Connected" );

        // main loop
        BufferedReader commandlineReader = new BufferedReader( new InputStreamReader( System.in ) );

        for ( ;; )
        {
            // get user input
            System.out.print( "> " );

            String input = null;

            try
            {
                input = commandlineReader.readLine();
            }
            catch ( IOException e )
            {
                throw new ClientException2( "Error encountered reading from commandline", e.getCause() );
            }

            if ( input.equals( "" ) )
            {
                continue;
            }

            if ( input.equals( "help" ) )
            {
                printHelpMessage( System.out );
                continue;
            }

            if ( input.equals( "quit" ) )
            {
                break;
            }

            String[] tokens = input.split( " " );

            long startTime = System.currentTimeMillis();

            // handle commands
            if ( tokens[0].equals( "table" ) )
            {
                if ( tokens.length == 1 )
                {
                    System.out.println( "Using table \"" + argTableName + "\"" );
                }
                else if ( tokens.length == 2 )
                {
                    argTableName = tokens[1];
                    System.out.println( "Using table \"" + argTableName + "\"" );
                }
                else
                {
                    System.out.println( "Error: syntax is \"table tablename\"" );
                }
            }
            else if ( tokens[0].equals( "read" ) )
            {
                if ( tokens.length == 1 )
                {
                    System.out.println( "Error: syntax is \"read keyname [field1 field2 ...]\"" );
                }
                else
                {
                    Set<String> fields = null;

                    if ( tokens.length > 2 )
                    {
                        fields = new HashSet<String>();

                        for ( int i = 2; i < tokens.length; i++ )
                        {
                            fields.add( tokens[i] );
                        }
                    }

                    HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
                    int returnCode = db.read( argTableName, tokens[1], fields, result );
                    System.out.println( "Return code: " + returnCode );
                    for ( Map.Entry<String, ByteIterator> resultEntry : result.entrySet() )
                    {
                        System.out.println( resultEntry.getKey() + "=" + resultEntry.getValue() );
                    }
                }
            }
            else if ( tokens[0].compareTo( "scan" ) == 0 )
            {
                if ( tokens.length < 3 )
                {
                    System.out.println( "Error: syntax is \"scan keyname scanlength [field1 field2 ...]\"" );
                }
                else
                {
                    Set<String> fields = null;

                    if ( tokens.length > 3 )
                    {
                        fields = new HashSet<String>();

                        for ( int i = 3; i < tokens.length; i++ )
                        {
                            fields.add( tokens[i] );
                        }
                    }

                    Vector<Map<String, ByteIterator>> results = new Vector<Map<String, ByteIterator>>();
                    int returnCode = db.scan( argTableName, tokens[1], Integer.parseInt( tokens[2] ), fields, results );
                    System.out.println( "Return code: " + returnCode );
                    int record = 0;
                    if ( results.size() == 0 )
                    {
                        System.out.println( "0 records" );
                    }
                    else
                    {
                        System.out.println( "--------------------------------" );
                    }
                    for ( Map<String, ByteIterator> result : results )
                    {
                        System.out.println( "Record " + ( record++ ) );
                        for ( Map.Entry<String, ByteIterator> resultEntry : result.entrySet() )
                        {
                            System.out.println( resultEntry.getKey() + "=" + resultEntry.getValue() );
                        }
                        System.out.println( "--------------------------------" );
                    }
                }
            }
            else if ( tokens[0].equals( "update" ) )
            {
                if ( tokens.length < 3 )
                {
                    System.out.println( "Error: syntax is \"update keyname name1=value1 [name2=value2 ...]\"" );
                }
                else
                {
                    HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

                    for ( int i = 2; i < tokens.length; i++ )
                    {
                        String[] nv = tokens[i].split( "=" );
                        values.put( nv[0], new StringByteIterator( nv[1] ) );
                    }

                    int ret = db.update( argTableName, tokens[1], values );
                    System.out.println( "Return code: " + ret );
                }
            }
            else if ( tokens[0].equals( "insert" ) )
            {
                if ( tokens.length < 3 )
                {
                    System.out.println( "Error: syntax is \"insert keyname name1=value1 [name2=value2 ...]\"" );
                }
                else
                {
                    HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

                    for ( int i = 2; i < tokens.length; i++ )
                    {
                        String[] nv = tokens[i].split( "=" );
                        values.put( nv[0], new StringByteIterator( nv[1] ) );
                    }

                    int returnCode = db.insert( argTableName, tokens[1], values );
                    System.out.println( "Return code: " + returnCode );
                }
            }
            else if ( tokens[0].equals( "delete" ) )
            {
                if ( tokens.length != 2 )
                {
                    System.out.println( "Error: syntax is \"delete keyname\"" );
                }
                else
                {
                    int returnCode = db.delete( argTableName, tokens[1] );
                    System.out.println( "Return code: " + returnCode );
                }
            }
            else
            {
                System.out.println( "Error: unknown command \"" + tokens[0] + "\"" );
            }

            System.out.println( ( System.currentTimeMillis() - startTime ) + " ms" );

        }
    }

}
