package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.workloads.ldbc.snb.bi.db.DummyLdbcSnbBiOperationInstances;
import com.ldbc.driver.workloads.ldbc.snb.bi.db.DummyLdbcSnbBiOperationResultSets;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@Ignore
public class BiSerializeMarshalOperationResultsTest
{
    @Test
    public void ldbcQuery1ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery1 operation = DummyLdbcSnbBiOperationInstances.read1();
        List<LdbcSnbBiQuery1Result> before = DummyLdbcSnbBiOperationResultSets.read1Results();

        // When
        List<LdbcSnbBiQuery1Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery2ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery2 operation = DummyLdbcSnbBiOperationInstances.read2();
        List<LdbcSnbBiQuery2Result> before = DummyLdbcSnbBiOperationResultSets.read2Results();

        // When
        List<LdbcSnbBiQuery2Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery3ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery3 operation = DummyLdbcSnbBiOperationInstances.read3();
        List<LdbcSnbBiQuery3Result> before = DummyLdbcSnbBiOperationResultSets.read3Results();

        // When
        List<LdbcSnbBiQuery3Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery4ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery4 operation = DummyLdbcSnbBiOperationInstances.read4();
        List<LdbcSnbBiQuery4Result> before = DummyLdbcSnbBiOperationResultSets.read4Results();

        // When
        List<LdbcSnbBiQuery4Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery5ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery5 operation = DummyLdbcSnbBiOperationInstances.read5();
        List<LdbcSnbBiQuery5Result> before = DummyLdbcSnbBiOperationResultSets.read5Results();

        // When
        List<LdbcSnbBiQuery5Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery6ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery6 operation = DummyLdbcSnbBiOperationInstances.read6();
        List<LdbcSnbBiQuery6Result> before = DummyLdbcSnbBiOperationResultSets.read6Results();

        // When
        List<LdbcSnbBiQuery6Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery7ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery7 operation = DummyLdbcSnbBiOperationInstances.read7();
        List<LdbcSnbBiQuery7Result> before = DummyLdbcSnbBiOperationResultSets.read7Results();

        // When
        List<LdbcSnbBiQuery7Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery8ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery8 operation = DummyLdbcSnbBiOperationInstances.read8();
        List<LdbcSnbBiQuery8Result> before = DummyLdbcSnbBiOperationResultSets.read8Results();

        // When
        List<LdbcSnbBiQuery8Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery9ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery9 operation = DummyLdbcSnbBiOperationInstances.read9();
        List<LdbcSnbBiQuery9Result> before = DummyLdbcSnbBiOperationResultSets.read9Results();

        // When
        List<LdbcSnbBiQuery9Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery10ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery10 operation = DummyLdbcSnbBiOperationInstances.read10();
        List<LdbcSnbBiQuery10Result> before = DummyLdbcSnbBiOperationResultSets.read10Results();

        // When
        List<LdbcSnbBiQuery10Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery11ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery11 operation = DummyLdbcSnbBiOperationInstances.read11();
        List<LdbcSnbBiQuery11Result> before = DummyLdbcSnbBiOperationResultSets.read11Results();

        // When
        List<LdbcSnbBiQuery11Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery12ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery12 operation = DummyLdbcSnbBiOperationInstances.read12();
        List<LdbcSnbBiQuery12Result> before = DummyLdbcSnbBiOperationResultSets.read12Results();

        // When
        List<LdbcSnbBiQuery12Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery13ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery13 operation = DummyLdbcSnbBiOperationInstances.read13();
        List<LdbcSnbBiQuery13Result> before = DummyLdbcSnbBiOperationResultSets.read13Results();

        // When
        List<LdbcSnbBiQuery13Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery14ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery14 operation = DummyLdbcSnbBiOperationInstances.read14();
        List<LdbcSnbBiQuery14Result> before = DummyLdbcSnbBiOperationResultSets.read14Results();

        // When
        List<LdbcSnbBiQuery14Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery15ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery15 operation = DummyLdbcSnbBiOperationInstances.read15();
        List<LdbcSnbBiQuery15Result> before = DummyLdbcSnbBiOperationResultSets.read15Results();

        // When
        List<LdbcSnbBiQuery15Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery16ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery16 operation = DummyLdbcSnbBiOperationInstances.read16();
        List<LdbcSnbBiQuery16Result> before = DummyLdbcSnbBiOperationResultSets.read16Results();

        // When
        List<LdbcSnbBiQuery16Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery17ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery17 operation = DummyLdbcSnbBiOperationInstances.read17();
        List<LdbcSnbBiQuery17Result> before = DummyLdbcSnbBiOperationResultSets.read17Results();

        // When
        List<LdbcSnbBiQuery17Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery18ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery18 operation = DummyLdbcSnbBiOperationInstances.read18();
        List<LdbcSnbBiQuery18Result> before = DummyLdbcSnbBiOperationResultSets.read18Results();

        // When
        List<LdbcSnbBiQuery18Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery19ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery19 operation = DummyLdbcSnbBiOperationInstances.read19();
        List<LdbcSnbBiQuery19Result> before = DummyLdbcSnbBiOperationResultSets.read19Results();

        // When
        List<LdbcSnbBiQuery19Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery20ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery20 operation = DummyLdbcSnbBiOperationInstances.read20();
        List<LdbcSnbBiQuery20Result> before = DummyLdbcSnbBiOperationResultSets.read20Results();

        // When
        List<LdbcSnbBiQuery20Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery21ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery21 operation = DummyLdbcSnbBiOperationInstances.read21();
        List<LdbcSnbBiQuery21Result> before = DummyLdbcSnbBiOperationResultSets.read21Results();

        // When
        List<LdbcSnbBiQuery21Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery22ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery22 operation = DummyLdbcSnbBiOperationInstances.read22();
        List<LdbcSnbBiQuery22Result> before = DummyLdbcSnbBiOperationResultSets.read22Results();

        // When
        List<LdbcSnbBiQuery22Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery23ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery23 operation = DummyLdbcSnbBiOperationInstances.read23();
        List<LdbcSnbBiQuery23Result> before = DummyLdbcSnbBiOperationResultSets.read23Results();

        // When
        List<LdbcSnbBiQuery23Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }

    @Test
    public void ldbcQuery24ShouldSerializeAndMarshalResult() throws SerializingMarshallingException
    {
        // Given
        LdbcSnbBiQuery24 operation = DummyLdbcSnbBiOperationInstances.read24();
        List<LdbcSnbBiQuery24Result> before = DummyLdbcSnbBiOperationResultSets.read24Results();

        // When
        List<LdbcSnbBiQuery24Result> after = operation.marshalResult(
                operation.serializeResult(
                        operation.marshalResult(
                                operation.serializeResult( before )
                        )
                )
        );

        // Then
        assertThat( before, equalTo( after ) );
    }
}
