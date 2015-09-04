package com.ldbc.driver.validation;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.DriverConfigurationFileHelper;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ValidationParamsGeneratorTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    // TODO remove
    @Ignore
    @Test
    public void
    generatedValidationFileLengthShouldEqualMinimumOfValidationSetSizeParamAndOperationsStreamLengthWhenBothAreEqual()
            throws IOException, DriverConfigurationException, WorkloadException, DbException
    {
        // Given
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        File tempValidationFile = temporaryFolder.newFile();

        ConsoleAndFileDriverConfiguration configuration = DriverConfigurationFileHelper.readConfigurationFileAt(
                DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArgs( LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1() );
        Map<String,String> additionalParamsMap = new HashMap<>();
        additionalParamsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/" ).getAbsolutePath() );
        additionalParamsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/" ).getAbsolutePath() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( additionalParamsMap );
        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArgs( MapUtils.loadPropertiesToMap( TestUtils.getResource(
                        "/snb/interactive/updateStream.properties" ) ) );


        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init( configuration );

        Db db = new DummyLdbcSnbInteractiveDb();
        db.init( configuration.asMap(), loggingService );

        List<Operation> operationsList = buildOperations();
        Iterator<Operation> operations = operationsList.iterator();

        int validationSetSize = 28;

        ValidationParamsGenerator validationParamsBefore = new ValidationParamsGenerator(
                db,
                workload.dbValidationParametersFilter( validationSetSize ),
                operations
        );
        List<ValidationParam> validationParamsBeforeList = Lists.newArrayList( validationParamsBefore );

        boolean performSerializationMarshallingChecks = true;
        Iterator<String[]> validationParamsAsCsvRows = new ValidationParamsToCsvRows(
                validationParamsBeforeList.iterator(),
                workload,
                performSerializationMarshallingChecks
        );

        try ( SimpleCsvFileWriter simpleCsvFileWriter =
                      new SimpleCsvFileWriter( tempValidationFile, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR ) )
        {
            simpleCsvFileWriter.writeRows( validationParamsAsCsvRows );
        }

        // TODO remove
        System.out.println(operationsList.size());
        System.out.println( Iterators.size( FileUtils.lineIterator( tempValidationFile ) ) );

        SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(
                tempValidationFile,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING
        );
        ValidationParamsFromCsvRows validationParamsAfter =
                new ValidationParamsFromCsvRows( simpleCsvFileReader, workload );
        List<ValidationParam> validationParamsAfterList = Lists.newArrayList( validationParamsAfter );

        int expectedValidationSetSize = Math.min( operationsList.size(), validationSetSize );

        assertThat( validationParamsAfterList.size(), is( validationParamsBeforeList.size() ) );
        assertThat( validationParamsAfterList.size(), is( expectedValidationSetSize ) );
        assertThat( validationParamsAfterList.size(), is( validationParamsBefore.entriesWrittenSoFar() ) );
        assertThat( validationParamsBeforeList.size(), is( validationParamsBefore.entriesWrittenSoFar() ) );

        for ( int i = 0; i < validationParamsAfterList.size(); i++ )
        {
            ValidationParam validationParamBefore = validationParamsBeforeList.get( i );
            ValidationParam validationParamAfter = validationParamsAfterList.get( i );
            assertThat( validationParamBefore, equalTo( validationParamAfter ) );
        }

        simpleCsvFileReader.close();
        System.out.println( tempValidationFile.getAbsolutePath() );
    }

    List<Operation> buildOperations()
    {
        return Lists.<Operation>newArrayList(
                DummyLdbcSnbInteractiveOperationInstances.read1(),
                DummyLdbcSnbInteractiveOperationInstances.read2(),
                DummyLdbcSnbInteractiveOperationInstances.read3(),
                DummyLdbcSnbInteractiveOperationInstances.read4(),
                DummyLdbcSnbInteractiveOperationInstances.read5(),
                DummyLdbcSnbInteractiveOperationInstances.read6(),
                DummyLdbcSnbInteractiveOperationInstances.read7(),
                DummyLdbcSnbInteractiveOperationInstances.read8(),
                DummyLdbcSnbInteractiveOperationInstances.read9(),
                DummyLdbcSnbInteractiveOperationInstances.read10(),
                DummyLdbcSnbInteractiveOperationInstances.read11(),
                DummyLdbcSnbInteractiveOperationInstances.read12(),
                DummyLdbcSnbInteractiveOperationInstances.read13(),
                DummyLdbcSnbInteractiveOperationInstances.read14(),
                DummyLdbcSnbInteractiveOperationInstances.read1(),
                DummyLdbcSnbInteractiveOperationInstances.read2(),
                DummyLdbcSnbInteractiveOperationInstances.read3(),
                DummyLdbcSnbInteractiveOperationInstances.read4(),
                DummyLdbcSnbInteractiveOperationInstances.read5(),
                DummyLdbcSnbInteractiveOperationInstances.read6(),
                DummyLdbcSnbInteractiveOperationInstances.read7(),
                DummyLdbcSnbInteractiveOperationInstances.read8(),
                DummyLdbcSnbInteractiveOperationInstances.read9(),
                DummyLdbcSnbInteractiveOperationInstances.read10(),
                DummyLdbcSnbInteractiveOperationInstances.read11(),
                DummyLdbcSnbInteractiveOperationInstances.read12(),
                DummyLdbcSnbInteractiveOperationInstances.read13(),
                DummyLdbcSnbInteractiveOperationInstances.read14()
        );
    }
}