package com.yahoo.ycsb.workloads;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.AbstractGeneratorFactory;
import com.yahoo.ycsb.generator.ConstantNumberGenerator;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.GeneratorFactory;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.UniformNumberGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.workloads.Distribution;
import com.yahoo.ycsb.workloads.WorkloadUtils;

public class WorkloadUtilsTest
{
    GeneratorFactory generatorFactory = null;

    @Before
    public void initGeneratorFactory()
    {
        generatorFactory = new AbstractGeneratorFactory( new RandomDataGenerator() ).newGeneratorFactory();
    }

    @Test
    public void buildAllValuesTest() throws WorkloadException
    {
        // Given
        int lowerBound = 3;
        int upperBound = 6;
        Generator<Long> valueLengthGenerator = generatorFactory.newUniformNumberGenerator( (long) lowerBound,
                (long) upperBound );
        int fieldCount = 10;
        String fieldNamePrefix = "field";

        // When
        HashMap<String, ByteIterator> values = WorkloadUtils.buildAllValues( fieldCount, valueLengthGenerator,
                fieldNamePrefix );

        // Then
        assertEquals( fieldCount, values.size() );
        for ( Entry<String, ByteIterator> entry : values.entrySet() )
        {
            assertEquals( true, entry.getKey().startsWith( fieldNamePrefix ) );
            assertInRange( lowerBound, entry.getValue().toString().length(), upperBound );
        }
    }

    @Test
    public void buildOneValueTest() throws WorkloadException
    {
        // Given
        int keyLowerBound = 1;
        int keyUpperBound = 3;
        Generator<Long> keyChooser = generatorFactory.newUniformNumberGenerator( (long) keyLowerBound,
                (long) keyUpperBound );

        int valueLengthLowerBound = 3;
        int valueLengthUpperBound = 6;
        Generator<Long> valueLengthGenerator = generatorFactory.newUniformNumberGenerator(
                (long) valueLengthLowerBound, (long) valueLengthUpperBound );

        String keyNamePrefix = "field";

        // When
        HashMap<String, ByteIterator> values = WorkloadUtils.buildOneValue( keyChooser, valueLengthGenerator,
                keyNamePrefix );

        // Then
        assertEquals( 1, values.size() );
        Entry<String, ByteIterator> entry = values.entrySet().iterator().next();
        assertEquals( true, entry.getKey().startsWith( keyNamePrefix ) );

        int keyNumber = Integer.parseInt( entry.getKey().substring( entry.getKey().length() - 1 ) );
        assertInRange( keyLowerBound, keyNumber, keyUpperBound );

        int valueLength = entry.getValue().toString().length();
        assertInRange( valueLengthLowerBound, valueLength, valueLengthUpperBound );
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
    public void buildConstantGeneratorTest() throws WorkloadException
    {
        // Given
        String histogramFilePath = null;

        // When
        Generator<Long> constantIntegerGenerator = WorkloadUtils.buildFieldLengthGenerator( Distribution.CONSTANT, 1l,
                10l, histogramFilePath );

        // Then
        assertEquals( ConstantNumberGenerator.class, constantIntegerGenerator.getClass() );
    }

    @Test
    public void buildUniformGeneratorTest() throws WorkloadException
    {
        // Given
        String histogramFilePath = null;

        // When
        Generator<Long> uniformLongGenerator = WorkloadUtils.buildFieldLengthGenerator( Distribution.UNIFORM, 1l, 10l,
                histogramFilePath );

        // Then
        assertEquals( UniformNumberGenerator.class, uniformLongGenerator.getClass() );
        assertEquals( UniformNumberGenerator.class, getGeneticTypeParameter( uniformLongGenerator.getClass() ) );
    }

    @Test
    public void buildZipfianGeneratorTest() throws WorkloadException
    {
        // Given
        String histogramFilePath = null;

        // When
        Generator<Long> zipfianIntegerGenerator = WorkloadUtils.buildFieldLengthGenerator( Distribution.ZIPFIAN, 1l,
                10l, histogramFilePath );

        // Then
        assertEquals( ZipfianGenerator.class, zipfianIntegerGenerator.getClass() );
    }

    @Ignore
    @Test
    public void buildHistogramGeneratorTest() throws WorkloadException, IOException
    {
        // Given
        String histogramFilePath = "/tmp/histogram";
        new File( "/tmp/histogram" ).createNewFile();
        // TODO populate histogram file with real data

        // When
        Generator<Long> histogramIntegerGenerator = WorkloadUtils.buildFieldLengthGenerator( Distribution.HISTOGRAM,
                1l, 10l, histogramFilePath );

        // Then
        assertEquals( HistogramGenerator.class, histogramIntegerGenerator.getClass() );
    }

    @Test( expected = WorkloadException.class )
    public void buildHistogramGeneratorWhenNoFileTest() throws WorkloadException
    {
        // Given
        String histogramFilePath = null;

        // When
        WorkloadUtils.buildFieldLengthGenerator( Distribution.HISTOGRAM, 1l, 10l, histogramFilePath );

        // Then
    }

    private void assertInRange( double lowerInclusive, double value, double upperInclusive )
    {
        assertEquals( "Should be above or equal to lower bound", true, ( lowerInclusive <= value ) );
        assertEquals( "Should be lower or equal to upper bound", true, ( value <= upperInclusive ) );
    }

    public Class getGeneticTypeParameter( Class genericClass )
    {
        ParameterizedType parameterizedType = (ParameterizedType) genericClass.getGenericSuperclass();
        return (Class) parameterizedType.getActualTypeArguments()[0];
    }
}
