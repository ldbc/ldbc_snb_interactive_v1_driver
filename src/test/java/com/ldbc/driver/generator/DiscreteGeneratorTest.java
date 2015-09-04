package com.ldbc.driver.generator;

import com.ldbc.driver.util.Bucket.DiscreteBucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class DiscreteGeneratorTest extends GeneratorTest<String, Integer> {

    @Override
    public Histogram<String, Integer> getExpectedDistribution() {
        Histogram<String, Integer> expectedDistribution = new Histogram<String, Integer>(0);
        expectedDistribution.addBucket(DiscreteBucket.create("1"), 1);
        expectedDistribution.addBucket(DiscreteBucket.create("2"), 2);
        expectedDistribution.addBucket(DiscreteBucket.create("3"), 4);
        expectedDistribution.addBucket(DiscreteBucket.create("4"), 8);
        return expectedDistribution;
    }

    @Override
    public double getDistributionTolerance() {
        return 0.01;
    }

    @Override
    public Iterator<String> getGeneratorImpl(GeneratorFactory generatorFactory) {
        Tuple2<Double, String> p1 = Tuple.tuple2(1.0, "1");
        Tuple2<Double, String> p2 = Tuple.tuple2(2.0, "2");
        Tuple2<Double, String> p3 = Tuple.tuple2(4.0, "3");
        Tuple2<Double, String> p4 = Tuple.tuple2(8.0, "4");
        ArrayList<Tuple2<Double, String>> items = new ArrayList<Tuple2<Double, String>>();
        items.add(p1);
        items.add(p2);
        items.add(p3);
        items.add(p4);
        return generatorFactory.weightedDiscrete(items);
    }

    @Test(expected = GeneratorException.class)
    public void emptyConstructorTest() {
        // Given
        GeneratorFactory generatorFactory = new GeneratorFactory(new RandomDataGeneratorFactory());
        ArrayList<Tuple2<Double, String>> emptyItems = new ArrayList<Tuple2<Double, String>>();
        Iterator<String> generator = generatorFactory.weightedDiscrete(emptyItems);

        // When
        generator.next();

        // Then
        assertEquals("Empty DiscreteGenerator should throw exception on next()", false, true);
    }
}
