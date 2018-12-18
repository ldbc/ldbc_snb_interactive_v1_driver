package com.ldbc.driver.control;

import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;

public class DriverConfigurationFileHelper
{
    public static void main( String[] args ) throws IOException, DriverConfigurationException
    {
        File baseConfigurationFilePublicLocation = new File( args[0] );
        updateDefaultConfigurationFiles(baseConfigurationFilePublicLocation);
        print();
    }

    private static void updateDefaultConfigurationFiles(File baseConfigurationFilePublicLocation) throws DriverConfigurationException, IOException
    {
        createBaseConfigurationAt( baseConfigurationFilePublicLocation );
        if ( !readConfigurationFileAt( baseConfigurationFilePublicLocation ).equals( baseConfiguration() ) )
        {
            throw new DriverConfigurationException(
                    format( "Default config file not equal to base configuration\n%s\n%s",
                            baseConfiguration().toString(),
                            readConfigurationFileAt( baseConfigurationFilePublicLocation ) ) );
        }
    }

    public static void print() throws DriverConfigurationException
    {
        System.out.println( ConsoleAndFileDriverConfiguration.commandlineHelpString() );
        System.out.println();
        System.out.println();
        System.out.println( ConsoleAndFileDriverConfiguration
                .fromDefaults( DummyLdbcSnbInteractiveDb.class.getName(), LdbcSnbInteractiveWorkload.class.getName(),
                        1000 ).toString() );
        System.out.println();
        System.out.println();
        System.out.println( ConsoleAndFileDriverConfiguration.fromDefaults( null, null, 0 ).toPropertiesString() );
    }


    private static void createBaseConfigurationAt( File baseConfigurationFile )
            throws IOException, DriverConfigurationException
    {
        // Delete old configuration file and create new one, in appropriate directory
        if ( baseConfigurationFile.exists() )
        {
            FileUtils.deleteQuietly( baseConfigurationFile );
        }
        if ( !baseConfigurationFile.createNewFile() )
        {
            throw new RuntimeException( "Unable to create file: " + baseConfigurationFile.getAbsolutePath() );
        }

        // Create base default configuration
        ConsoleAndFileDriverConfiguration defaultsOnly = baseConfiguration();

        // Write configuration to file
        new FileOutputStream( baseConfigurationFile ).write( defaultsOnly.toPropertiesString().getBytes() );

        System.out.println( "New configuration file written to " + baseConfigurationFile.getAbsolutePath() );
    }

    private static ConsoleAndFileDriverConfiguration readConfigurationFileAt( File configurationFile )
            throws IOException, DriverConfigurationException
    {
        if ( !configurationFile.exists() )
        {
            throw new DriverConfigurationException(
                    "Config file does not exist: " + configurationFile.getAbsolutePath() );
        }

        Properties ldbcDriverDefaultConfigurationProperties = new Properties();
        ldbcDriverDefaultConfigurationProperties.load( new FileInputStream( configurationFile ) );
        Map<String,String> ldbcDriverDefaultConfigurationAsParamsMap =
                ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(
                        MapUtils.propertiesToMap( ldbcDriverDefaultConfigurationProperties )
                );

        if ( !ldbcDriverDefaultConfigurationAsParamsMap
                .containsKey( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG ) )
        { ldbcDriverDefaultConfigurationAsParamsMap.put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "0" ); }

        return ConsoleAndFileDriverConfiguration.fromParamsMap( ldbcDriverDefaultConfigurationAsParamsMap );
    }

    static File getBaseConfigurationFilePublicLocation()
    {
        return org.apache.commons.io.FileUtils.toFile(
                DriverConfigurationFileHelper.class.getResource( "/configuration/ldbc_driver_default.properties" ));
    }

    private static ConsoleAndFileDriverConfiguration baseConfiguration() throws DriverConfigurationException
    {
        String databaseClassName = null;
        String workloadClassName = null;
        long operationCount = 0;
        return ConsoleAndFileDriverConfiguration.fromDefaults( databaseClassName, workloadClassName, operationCount );
    }
}
