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
    private static final String CONFIGURATION_DIR_NAME = "configuration";

    public static void main( String[] args ) throws IOException, DriverConfigurationException
    {
        updateDefaultConfigurationFiles();
        print();
    }

    public static void updateDefaultConfigurationFiles() throws DriverConfigurationException, IOException
    {
        File driverRootDirectory = getDriverRootDirectory();

        File baseConfigurationFilePublicLocation = getBaseConfigurationFilePublicLocation( driverRootDirectory );
        createBaseConfigurationAt( baseConfigurationFilePublicLocation );
        if ( false == readConfigurationFileAt( baseConfigurationFilePublicLocation ).equals( baseConfiguration() ) )
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


    public static void createBaseConfigurationAt( File baseConfigurationFile )
            throws IOException, DriverConfigurationException
    {
        // Delete old configuration file and create new one, in appropriate directory
        if ( baseConfigurationFile.exists() )
        { FileUtils.deleteQuietly( baseConfigurationFile ); }
        baseConfigurationFile.createNewFile();

        // Create base default configuration
        ConsoleAndFileDriverConfiguration defaultsOnly = baseConfiguration();

        // Write configuration to file
        new FileOutputStream( baseConfigurationFile ).write( defaultsOnly.toPropertiesString().getBytes() );

        System.out.println( "New configuration file written to " + baseConfigurationFile.getAbsolutePath() );
    }

    public static ConsoleAndFileDriverConfiguration readConfigurationFileAt( File configurationFile )
            throws IOException, DriverConfigurationException
    {
        if ( false == configurationFile.exists() )
        {
            throw new DriverConfigurationException(
                    "Config file does not exist: " + configurationFile.getAbsolutePath() );
        }

        Properties ldbcDriverDefaultConfigurationProperties = new Properties();
        ldbcDriverDefaultConfigurationProperties.load( new FileInputStream( configurationFile ) );
        Map<String,String> ldbcDriverDefaultConfigurationAsParamsMap =
                ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(
                        MapUtils.<String,String>propertiesToMap( ldbcDriverDefaultConfigurationProperties )
                );

        if ( false == ldbcDriverDefaultConfigurationAsParamsMap
                .containsKey( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG ) )
        { ldbcDriverDefaultConfigurationAsParamsMap.put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "0" ); }

        return ConsoleAndFileDriverConfiguration.fromParamsMap( ldbcDriverDefaultConfigurationAsParamsMap );
    }

    public static File getBaseConfigurationFilePublicLocation() throws DriverConfigurationException
    {
        return getBaseConfigurationFilePublicLocation( getDriverRootDirectory() );
    }

    public static File getWorkloadsDirectory() throws DriverConfigurationException
    {
        File rootDirectory = getDriverRootDirectory();
        return getWorkloadsDirectory( rootDirectory );
    }

    public static File getWorkloadsDirectory( File driverRootDirectory ) throws DriverConfigurationException
    {
        File workloadsDirectory = new File( driverRootDirectory, CONFIGURATION_DIR_NAME );
        if ( false == workloadsDirectory.exists() )
        {
            throw new DriverConfigurationException(
                    "Directory does not exist: " + workloadsDirectory.getAbsolutePath() );
        }
        if ( false == workloadsDirectory.isDirectory() )
        {
            throw new DriverConfigurationException(
                    "Directory not a directory: " + workloadsDirectory.getAbsolutePath() );
        }
        return workloadsDirectory;
    }

    private static ConsoleAndFileDriverConfiguration baseConfiguration() throws DriverConfigurationException
    {
        String databaseClassName = null;
        String workloadClassName = null;
        long operationCount = 0;
        return ConsoleAndFileDriverConfiguration.fromDefaults( databaseClassName, workloadClassName, operationCount );
    }

    private static File getDriverRootDirectory()
    {
        File resourcesDirectory = FileUtils.toFile( DriverConfigurationFileHelper.class.getResource( "/" ) );
        File targetDirectory = resourcesDirectory.getParentFile();
        return targetDirectory.getParentFile();
    }

    private static File getBaseConfigurationFilePublicLocation( File driverRootDirectory )
            throws DriverConfigurationException
    {
        File workloadsDirectory = new File( driverRootDirectory, CONFIGURATION_DIR_NAME );
        if ( false == workloadsDirectory.exists() )
        {
            throw new DriverConfigurationException(
                    "Directory does not exist: " + workloadsDirectory.getAbsolutePath() );
        }
        return new File( workloadsDirectory, "ldbc_driver_default.properties" );
    }
}