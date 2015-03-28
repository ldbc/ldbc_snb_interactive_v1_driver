package com.ldbc.driver.validation;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.DriverConfigurationFileHelper;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.csv.SimpleCsvFileReader;
import com.ldbc.driver.util.csv.SimpleCsvFileWriter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
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

public class ValidationParamsGeneratorTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    // TODO remove
    @Ignore
    @Test
    public void generatedValidationFileLengthShouldEqualMinimumOfValidationSetSizeParamAndOperationsStreamLengthWhenBothAreEqual() throws IOException, DriverConfigurationException, WorkloadException, DbException {
        // Given
        File tempValidationFile = temporaryFolder.newFile();

        ConsoleAndFileDriverConfiguration configuration = DriverConfigurationFileHelper.readConfigurationFileAt(
                DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation());
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(LdbcSnbInteractiveConfiguration.defaultConfig());
        Map<String, String> additionalParamsMap = new HashMap<>();
        additionalParamsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        additionalParamsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(additionalParamsMap);
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")));


        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);

        Db db = new DummyLdbcSnbInteractiveDb();
        db.init(configuration.asMap());

        List<Operation<?>> operationsList = buildOperations();
        Iterator<Operation<?>> operations = operationsList.iterator();

        int validationSetSize = 28;

        ValidationParamsGenerator validationParamsBefore = new ValidationParamsGenerator(db, workload.dbValidationParametersFilter(validationSetSize), operations);
        List<ValidationParam> validationParamsBeforeList = Lists.newArrayList(validationParamsBefore);

        Iterator<String[]> validationParamsAsCsvRows = new ValidationParamsToCsvRows(validationParamsBeforeList.iterator(), workload, true);

        SimpleCsvFileWriter simpleCsvFileWriter = new SimpleCsvFileWriter(tempValidationFile, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR);
        simpleCsvFileWriter.writeRows(validationParamsAsCsvRows);
        simpleCsvFileWriter.close();

        SimpleCsvFileReader simpleCsvFileReader = new SimpleCsvFileReader(tempValidationFile, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING);
        ValidationParamsFromCsvRows validationParamsAfter = new ValidationParamsFromCsvRows(simpleCsvFileReader, workload);
        List<ValidationParam> validationParamsAfterList = Lists.newArrayList(validationParamsAfter);

        int expectedValidationSetSize = Math.min(operationsList.size(), validationSetSize);

        assertThat(validationParamsAfterList.size(), is(validationParamsBeforeList.size()));
        assertThat(validationParamsAfterList.size(), is(expectedValidationSetSize));
        assertThat(validationParamsAfterList.size(), is(validationParamsBefore.entriesWrittenSoFar()));
        assertThat(validationParamsBeforeList.size(), is(validationParamsBefore.entriesWrittenSoFar()));

        for (int i = 0; i < validationParamsAfterList.size(); i++) {
            ValidationParam validationParamBefore = validationParamsBeforeList.get(i);
            ValidationParam validationParamAfter = validationParamsAfterList.get(i);
            assertThat(validationParamBefore, equalTo(validationParamAfter));
        }

        simpleCsvFileReader.close();
        System.out.println(tempValidationFile.getAbsolutePath());
    }

    List<Operation<?>> buildOperations() {
        return Lists.<Operation<?>>newArrayList(
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