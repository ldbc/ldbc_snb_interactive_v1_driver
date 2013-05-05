package com.yahoo.ycsb.workloads;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.GeneratorBuilder;
import com.yahoo.ycsb.generator.GeneratorBuilderFactory;
import com.yahoo.ycsb.generator.ConstantGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.UniformNumberGenerator;
import com.yahoo.ycsb.generator.ycsb.ZipfianGenerator;
import com.yahoo.ycsb.util.ByteIterator;
import com.yahoo.ycsb.workloads.Distribution;
import com.yahoo.ycsb.workloads.WorkloadUtils;

public class WorkloadUtilsTest
{
    GeneratorBuilder generatorBuilder = null;

    @Before
    public void initGeneratorFactory()
    {
        generatorBuilder = new GeneratorBuilderFactory( new RandomDataGenerator() ).newGeneratorBuilder();
    }

    @Test
    public void buildValuedFieldsReturnAllTest() throws WorkloadException
    {
        // Given
        int fieldLength = 3;
        int fieldCount = 10;
        boolean returnAllFields = true;
        String fieldNamePrefix = "field";

        // When
        Generator<Set<String>> fieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator( generatorBuilder,
                fieldNamePrefix, fieldCount, returnAllFields );

        Generator<Integer> constantFieldLengthGenerator = WorkloadUtils.buildFieldLengthGenerator( generatorBuilder,
                Distribution.CONSTANT, fieldLength, fieldLength );

        Map<String, ByteIterator> valuedFields = WorkloadUtils.buildValuedFields( fieldSelectionGenerator,
                constantFieldLengthGenerator );

        // Then
        assertEquals( fieldCount, valuedFields.size() );
        for ( Entry<String, ByteIterator> entry : valuedFields.entrySet() )
        {
            assertEquals( true, entry.getKey().startsWith( fieldNamePrefix ) );
            assertInRange( fieldLength, entry.getValue().toString().length(), fieldLength );
        }
    }

    @Ignore
    @Test
    public void nextKeyNumberTest()
    {
        // Given
        // int keyLowerBound = 1;
        // int keyUpperBound = 3;
        // IntegerGenerator keyChooser = new UniformIntegerGenerator(
        // keyLowerBound, keyUpperBound );
        // int countStart = 10;
        // IntegerGenerator keySequence = new CounterGenerator( countStart );

        // to test it
        // for ( int i = 0; i < 10000; i++ )
        // {
        // int c = keyChooser.nextInt();
        // int s = keySequence.nextInt();
        //
        // System.out.println( c + ":" + s + ":" + WorkloadUtils.nextKeyNumber(
        // keyChooser, keySequence ) );
        // }

        // When
        // int nextKeyNumber = WorkloadUtils.nextKeyNumber( keyChooser,
        // keySequence );

        // Then
        assertEquals( false, true );
    }

    @Test
    public void buildConstantFieldLengthGeneratorTest() throws WorkloadException
    {
        // Given
        int lowerBound = 1;
        int upperBound = 1;
        Distribution distribution = Distribution.CONSTANT;

        // When
        Generator<Integer> constantGenerator = WorkloadUtils.buildFieldLengthGenerator( generatorBuilder, distribution,
                lowerBound, upperBound );

        // Then
        assertEquals( ConstantGenerator.class, constantGenerator.getClass() );
    }

    @Test
    public void buildUniformFieldLengthGeneratorTest() throws WorkloadException
    {
        // Given
        long lowerBound = 1;
        long upperBound = 1;
        Distribution distribution = Distribution.UNIFORM;

        // When
        Generator<Long> uniformLongGenerator = WorkloadUtils.buildFieldLengthGenerator( generatorBuilder, distribution,
                lowerBound, upperBound );

        // Then
        assertEquals( UniformNumberGenerator.class, uniformLongGenerator.getClass() );
    }

    @Test
    public void buildZipfianFieldLengthGeneratorTest() throws WorkloadException
    {
        // Given
        long lowerBound = 1;
        long upperBound = 1;
        Distribution distribution = Distribution.UNIFORM;

        // When
        Generator<Long> zipfianIntegerGenerator = WorkloadUtils.buildFieldLengthGenerator( generatorBuilder,
                distribution, lowerBound, upperBound );

        // Then
        assertEquals( ZipfianGenerator.class, zipfianIntegerGenerator.getClass() );
    }

    @Test
    public void buildFieldSelectionGeneratorTest()
    {
        assertEquals( false, true );
    }

    private void assertInRange( double lowerInclusive, double value, double upperInclusive )
    {
        assertEquals( "Should be above or equal to lower bound", true, ( lowerInclusive <= value ) );
        assertEquals( "Should be lower or equal to upper bound", true, ( value <= upperInclusive ) );
    }
}
