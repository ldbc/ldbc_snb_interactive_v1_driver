package com.ldbc.driver.workloads;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import OLD_com.ldbc.driver.workloads.WorkloadException;
import OLD_com.ldbc.driver.workloads.ycsb.Distribution;
import OLD_com.ldbc.driver.workloads.ycsb.WorkloadUtils;

import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.generator.ConstantGenerator;
import com.ldbc.driver.generator.DynamicRangeUniformNumberGenerator;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.generator.ycsb.YcsbZipfianNumberGenerator;
import com.ldbc.driver.util.RandomDataGeneratorFactory;

public class WorkloadUtilsTest
{
    private final long RANDOM_SEED = 42;
    GeneratorBuilder generatorBuilder = null;

    @Before
    public void initGeneratorFactory()
    {
        generatorBuilder = new GeneratorBuilder( new RandomDataGeneratorFactory( RANDOM_SEED ) );
    }

    @Test
    public void buildFieldSelectionGeneratorTest() throws WorkloadException
    {
        // Given
        int fieldCount = 10;
        String fieldNamePrefix = "field";

        // When
        Generator<Set<String>> singleFieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator(
                generatorBuilder, fieldNamePrefix, fieldCount, 2 );
        Generator<Set<String>> allFieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator(
                generatorBuilder, fieldNamePrefix, fieldCount, fieldCount );

        // Then
        assertEquals( 2, singleFieldSelectionGenerator.next().size() );
        assertEquals( 10, allFieldSelectionGenerator.next().size() );
    }

    @Test
    public void buildValuedFieldsReturnAllTest() throws WorkloadException
    {
        // Given
        int fieldLength = 3;
        int fieldCount = 10;
        String fieldNamePrefix = "field";

        // When
        Generator<Set<String>> fieldSelectionGenerator = WorkloadUtils.buildFieldSelectionGenerator( generatorBuilder,
                fieldNamePrefix, fieldCount, fieldCount );

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
        assertEquals( DynamicRangeUniformNumberGenerator.class, uniformLongGenerator.getClass() );
    }

    @Test
    public void buildZipfianFieldLengthGeneratorTest() throws WorkloadException
    {
        // Given
        long lowerBound = 1;
        long upperBound = 1;
        Distribution distribution = Distribution.ZIPFIAN;

        // When
        Generator<Long> zipfianGenerator = WorkloadUtils.buildFieldLengthGenerator( generatorBuilder, distribution,
                lowerBound, upperBound );

        // Then
        assertEquals( YcsbZipfianNumberGenerator.class, zipfianGenerator.getClass() );
    }

    private void assertInRange( double lowerInclusive, double value, double upperInclusive )
    {
        assertEquals( "Should be above or equal to lower bound", true, ( lowerInclusive <= value ) );
        assertEquals( "Should be lower or equal to upper bound", true, ( value <= upperInclusive ) );
    }
}
