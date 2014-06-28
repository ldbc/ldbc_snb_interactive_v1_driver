package com.ldbc.driver.validation;

import com.google.common.collect.Lists;
import com.ldbc.driver.Workload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationResultInstances;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ValidationParamsToCsvRowsToValidationParamsTests {
    @Test
    public void validationParametersShouldBeUnchangedAfterSerializingAndMarshalling() {
        // Given
        Workload workload = new LdbcSnbInteractiveWorkload();
        List<ValidationParam> validationParamsBeforeBeforeSerializing = buildParams();

        // When
        List<String[]> serializedValidationParamsAsCsvRows =
                Lists.newArrayList(new ValidationParamsToCsvRows(validationParamsBeforeBeforeSerializing.iterator(), workload, true));
        List<ValidationParam> validationParamsAfterSerializingAndMarshalling =
                Lists.newArrayList(new ValidationParamsFromCsvRows(serializedValidationParamsAsCsvRows.iterator(), workload));

        // Then
        assertThat(validationParamsBeforeBeforeSerializing, equalTo(validationParamsAfterSerializingAndMarshalling));
    }

    List<ValidationParam> buildParams() {
        LdbcQuery1 operation1 = DummyLdbcSnbInteractiveOperationInstances.read1();
        List<LdbcQuery1Result> result1 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read1Result()
        );
        ValidationParam validationParam1 = new ValidationParam(operation1, result1);

        LdbcQuery2 operation2 = DummyLdbcSnbInteractiveOperationInstances.read2();
        List<LdbcQuery2Result> result2 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read2Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read2Result()
        );
        ValidationParam validationParam2 = new ValidationParam(operation2, result2);

        LdbcQuery3 operation3 = DummyLdbcSnbInteractiveOperationInstances.read3();
        List<LdbcQuery3Result> result3 = Lists.newArrayList();
        ValidationParam validationParam3 = new ValidationParam(operation3, result3);

        LdbcQuery4 operation4 = DummyLdbcSnbInteractiveOperationInstances.read4();
        List<LdbcQuery4Result> result4 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read4Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read4Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read4Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read4Result()
        );
        ValidationParam validationParam4 = new ValidationParam(operation4, result4);

        LdbcQuery5 operation5 = DummyLdbcSnbInteractiveOperationInstances.read5();
        List<LdbcQuery5Result> result5 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read5Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read5Result()
        );
        ValidationParam validationParam5 = new ValidationParam(operation5, result5);

        LdbcQuery6 operation6 = DummyLdbcSnbInteractiveOperationInstances.read6();
        List<LdbcQuery6Result> result6 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read6Result()
        );
        ValidationParam validationParam6 = new ValidationParam(operation6, result6);

        LdbcQuery7 operation7 = DummyLdbcSnbInteractiveOperationInstances.read7();
        List<LdbcQuery7Result> result7 = Lists.newArrayList(
        );
        ValidationParam validationParam7 = new ValidationParam(operation7, result7);

        LdbcQuery8 operation8 = DummyLdbcSnbInteractiveOperationInstances.read8();
        List<LdbcQuery8Result> result8 = Lists.newArrayList(
        );
        ValidationParam validationParam8 = new ValidationParam(operation8, result8);

        LdbcQuery9 operation9 = DummyLdbcSnbInteractiveOperationInstances.read9();
        List<LdbcQuery9Result> result9 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read9Result()
        );
        ValidationParam validationParam9 = new ValidationParam(operation9, result9);

        LdbcQuery10 operation10 = DummyLdbcSnbInteractiveOperationInstances.read10();
        List<LdbcQuery10Result> result10 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read10Result()
        );
        ValidationParam validationParam10 = new ValidationParam(operation10, result10);

        LdbcQuery11 operation11 = DummyLdbcSnbInteractiveOperationInstances.read11();
        List<LdbcQuery11Result> result11 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read11Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read11Result()
        );
        ValidationParam validationParam11 = new ValidationParam(operation11, result11);

        LdbcQuery12 operation12 = DummyLdbcSnbInteractiveOperationInstances.read12();
        List<LdbcQuery12Result> result12 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read12Result()
        );
        ValidationParam validationParam12 = new ValidationParam(operation12, result12);

        LdbcQuery13 operation13 = DummyLdbcSnbInteractiveOperationInstances.read13();
        List<LdbcQuery13Result> result13 = Lists.newArrayList(
        );
        ValidationParam validationParam13 = new ValidationParam(operation13, result13);

        LdbcQuery14 operation14 = DummyLdbcSnbInteractiveOperationInstances.read14();
        List<LdbcQuery14Result> result14 = Lists.newArrayList(
                DummyLdbcSnbInteractiveOperationResultInstances.read14Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read14Result(),
                DummyLdbcSnbInteractiveOperationResultInstances.read14Result()
        );
        ValidationParam validationParam14 = new ValidationParam(operation14, result14);

        return Lists.newArrayList(
                validationParam1,
                validationParam2,
                validationParam3,
                validationParam4,
                validationParam5,
                validationParam6,
                validationParam7,
                validationParam8,
                validationParam9,
                validationParam10,
                validationParam11,
                validationParam12,
                validationParam13,
                validationParam14
        );
    }
}
