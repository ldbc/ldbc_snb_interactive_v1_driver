package com.ldbc.driver.validation;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.DriverConfigurationFileTestHelper;
import com.ldbc.driver.util.CsvFileReader;
import com.ldbc.driver.util.CsvFileWriter;
import com.ldbc.driver.util.TestUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

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

    @Test
    public void generatedValidationFileLengthShouldEqualMinimumOfValidationSetSizeParamAndOperationsStreamLengthWhenBothAreEqual() throws IOException, DriverConfigurationException, WorkloadException, DbException {
        // Given
        File tempValidationFile = new File(TestUtils.getResource("/") + "temp_validation_file.csv");
        FileUtils.deleteQuietly(tempValidationFile);
        tempValidationFile.createNewFile();

        ConsoleAndFileDriverConfiguration configuration = DriverConfigurationFileTestHelper.readConfigurationFileAt(
                DriverConfigurationFileTestHelper.getBaseConfigurationFilePublicLocation());
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(LdbcSnbInteractiveWorkload.defaultReadOnlyConfig());
        Map<String, String> additionalParamsMap = new HashMap<>();
        additionalParamsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(additionalParamsMap);


        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);

        Db db = new DummyDb();
        db.init(new HashMap<String, String>());

        List<Operation<?>> operationsList = buildOperations();
        Iterator<Operation<?>> operations = operationsList.iterator();

        int validationSetSize = 25;

        ValidationParamsGenerator validationParamsBefore = new ValidationParamsGenerator(db, workload.dbValidationParametersFilter(validationSetSize), operations);
        List<ValidationParam> validationParamsBeforeList = Lists.newArrayList(validationParamsBefore);

        Iterator<String[]> validationParamsAsCsvRows = new ValidationParamsToCsvRows(validationParamsBeforeList.iterator(), workload, true);

        CsvFileWriter csvFileWriter = new CsvFileWriter(tempValidationFile, CsvFileWriter.DEFAULT_COLUMN_SEPARATOR_STRING);
        csvFileWriter.writeRows(validationParamsAsCsvRows);
        csvFileWriter.close();

        CsvFileReader csvFileReader = new CsvFileReader(tempValidationFile, CsvFileWriter.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING);
        ValidationParamsFromCsvRows validationParamsAfter = new ValidationParamsFromCsvRows(csvFileReader, workload);
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

        csvFileReader.closeReader();
        System.out.println(tempValidationFile.getAbsolutePath());
        FileUtils.deleteQuietly(tempValidationFile);
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