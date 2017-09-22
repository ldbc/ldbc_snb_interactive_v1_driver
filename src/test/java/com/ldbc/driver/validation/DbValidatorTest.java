package com.ldbc.driver.validation;

import com.google.common.collect.Lists;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationResultInstances;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationResultSets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class DbValidatorTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldFailValidationWhenDbImplementationIsIncorrect()
            throws DbException, WorkloadException, IOException, DriverConfigurationException
    {
        // Given
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        long operationCount = 1;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( dbClassName, workloadClassName, operationCount );

        Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1();
        paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
        paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( paramsMap );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                MapUtils.loadPropertiesToMap( TestUtils.getResource( "/snb/interactive/updateStream.properties" ) )
        );

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init( configuration );

        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
        List<ValidationParam> correctValidationParamsList =
                Lists.newArrayList( gf.limit( gf.repeating( buildParams().iterator() ), 10000 ) );

        LdbcQuery14 operation14 = DummyLdbcSnbInteractiveOperationInstances.read14();
        List<LdbcQuery14Result> unexpectedResult14 = DummyLdbcSnbInteractiveOperationResultSets.read14Results();
        unexpectedResult14.add( DummyLdbcSnbInteractiveOperationResultInstances.read14Result() );

        ValidationParam unexpectedValidationParam14 = ValidationParam.createTyped( operation14, unexpectedResult14 );
        correctValidationParamsList.add( unexpectedValidationParam14 );

        Iterator<ValidationParam> validationParams = correctValidationParamsList.iterator();
        Db db = new DummyLdbcSnbInteractiveDb();
        db.init(
                new HashMap<String,String>(),
                loggingService,
                workload.operationTypeToClassMapping()
        );
        DbValidator dbValidator = new DbValidator();

        // When
        DbValidationResult validationResult = dbValidator.validate(
                validationParams,
                db,
                correctValidationParamsList.size(),
                workload
        );

        // Then
        System.out.println( validationResult.resultMessage() );
        assertThat( validationResult.isSuccessful(), is( false ) );
    }

    @Test
    public void shouldPassValidationWhenDbImplementationIsCorrect()
            throws WorkloadException, DbException, IOException, DriverConfigurationException
    {
        // Given
        LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
        long operationCount = 1;
        ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                operationCount
        );

        Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1();
        paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
        paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( paramsMap );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                MapUtils.loadPropertiesToMap( TestUtils.getResource( "/snb/interactive/updateStream.properties" ) )
        );

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init( configuration );

        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
        Iterator<ValidationParam> validationParams = gf.limit(
                gf.repeating( buildParams().iterator() ),
                10000
        );

        Db db = new DummyLdbcSnbInteractiveDb();
        db.init(
                new HashMap<String,String>(),
                loggingService,
                workload.operationTypeToClassMapping()
        );

        DbValidator dbValidator = new DbValidator();

        // When
        DbValidationResult validationResult = dbValidator.validate(
                validationParams,
                db,
                10000,
                workload
        );

        // Then
        System.out.println( validationResult.resultMessage() );
        assertThat( format( "Validation Result\n%s", validationResult.resultMessage() ),
                validationResult.isSuccessful(), is( true ) );
    }

    List<ValidationParam> buildParams()
    {
        ValidationParam validationParamLong1 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read1(),
                DummyLdbcSnbInteractiveOperationResultSets.read1Results()
        );

        ValidationParam validationParamLong2 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read2(),
                DummyLdbcSnbInteractiveOperationResultSets.read2Results()
        );

        ValidationParam validationParamLong3 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read3(),
                DummyLdbcSnbInteractiveOperationResultSets.read3Results()
        );

        ValidationParam validationParamLong4 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read4(),
                DummyLdbcSnbInteractiveOperationResultSets.read4Results()
        );

        ValidationParam validationParamLong5 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read5(),
                DummyLdbcSnbInteractiveOperationResultSets.read5Results()
        );

        ValidationParam validationParamLong6 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read6(),
                DummyLdbcSnbInteractiveOperationResultSets.read6Results()
        );

        ValidationParam validationParamLong7 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read7(),
                DummyLdbcSnbInteractiveOperationResultSets.read7Results()
        );

        ValidationParam validationParamLong8 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read8(),
                DummyLdbcSnbInteractiveOperationResultSets.read8Results()
        );

        ValidationParam validationParamLong9 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read9(),
                DummyLdbcSnbInteractiveOperationResultSets.read9Results()
        );

        ValidationParam validationParamLong10 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read10(),
                DummyLdbcSnbInteractiveOperationResultSets.read10Results()
        );

        ValidationParam validationParamLong11 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read11(),
                DummyLdbcSnbInteractiveOperationResultSets.read11Results()
        );

        ValidationParam validationParamLong12 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read12(),
                DummyLdbcSnbInteractiveOperationResultSets.read12Results()
        );

        ValidationParam validationParamLong13 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read13(),
                DummyLdbcSnbInteractiveOperationResultSets.read13Results()
        );

        ValidationParam validationParamLong14 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.read14(),
                DummyLdbcSnbInteractiveOperationResultSets.read14Results()
        );

        ValidationParam validationParamShort1 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.short1(),
                DummyLdbcSnbInteractiveOperationResultSets.short1Results()
        );

        ValidationParam validationParamShort2 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.short2(),
                DummyLdbcSnbInteractiveOperationResultSets.short2Results()
        );

        ValidationParam validationParamShort3 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.short3(),
                DummyLdbcSnbInteractiveOperationResultSets.short3Results()
        );

        ValidationParam validationParamShort4 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.short4(),
                DummyLdbcSnbInteractiveOperationResultSets.short4Results()
        );

        ValidationParam validationParamShort5 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.short5(),
                DummyLdbcSnbInteractiveOperationResultSets.short5Results()
        );

        ValidationParam validationParamShort6 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.short6(),
                DummyLdbcSnbInteractiveOperationResultSets.short6Results()
        );

        ValidationParam validationParamShort7 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.short7(),
                DummyLdbcSnbInteractiveOperationResultSets.short7Results()
        );

        ValidationParam validationParamWrite1 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.write1(),
                LdbcNoResult.INSTANCE
        );

        ValidationParam validationParamWrite2 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.write2(),
                LdbcNoResult.INSTANCE
        );

        ValidationParam validationParamWrite3 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.write3(),
                LdbcNoResult.INSTANCE
        );

        ValidationParam validationParamWrite4 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.write4(),
                LdbcNoResult.INSTANCE
        );

        ValidationParam validationParamWrite5 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.write5(),
                LdbcNoResult.INSTANCE
        );

        ValidationParam validationParamWrite6 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.write6(),
                LdbcNoResult.INSTANCE
        );

        ValidationParam validationParamWrite7 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.write7(),
                LdbcNoResult.INSTANCE
        );

        ValidationParam validationParamWrite8 = ValidationParam.createTyped(
                DummyLdbcSnbInteractiveOperationInstances.write8(),
                LdbcNoResult.INSTANCE
        );

        return Lists.newArrayList(
                validationParamLong1,
                validationParamLong2,
                validationParamLong3,
                validationParamLong4,
                validationParamLong5,
                validationParamLong6,
                validationParamLong7,
                validationParamLong8,
                validationParamLong9,
                validationParamLong10,
                validationParamLong11,
                validationParamLong12,
                validationParamLong13,
                validationParamLong14,
                validationParamShort1,
                validationParamShort2,
                validationParamShort3,
                validationParamShort4,
                validationParamShort5,
                validationParamShort6,
                validationParamShort7,
                validationParamWrite1,
                validationParamWrite2,
                validationParamWrite3,
                validationParamWrite4,
                validationParamWrite5,
                validationParamWrite6,
                validationParamWrite7,
                validationParamWrite8
        );
    }
}
