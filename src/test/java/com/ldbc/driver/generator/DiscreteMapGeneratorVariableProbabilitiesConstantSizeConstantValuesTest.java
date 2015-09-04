package com.ldbc.driver.generator;

import com.ldbc.driver.util.Bucket.DiscreteBucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DiscreteMapGeneratorVariableProbabilitiesConstantSizeConstantValuesTest extends
        GeneratorTest<Map<String, Long>, Integer> {
    @Override
    public Histogram<Map<String, Long>, Integer> getExpectedDistribution() {
        // 12 & 21
        Map<String, Long> m12 = new HashMap<String, Long>();
        m12.put("1", 1l);
        m12.put("2", 2l);

        // 13 & 31
        Map<String, Long> m13 = new HashMap<String, Long>();
        m13.put("1", 1l);
        m13.put("3", 3l);

        // 23 & 32
        Map<String, Long> m23 = new HashMap<String, Long>();
        m23.put("2", 2l);
        m23.put("3", 3l);

        Histogram<Map<String, Long>, Integer> expectedDistribution = new Histogram<Map<String, Long>, Integer>(0);

        expectedDistribution.addBucket(DiscreteBucket.create(m12), 25);
        expectedDistribution.addBucket(DiscreteBucket.create(m13), 10);
        expectedDistribution.addBucket(DiscreteBucket.create(m23), 25);
        return expectedDistribution;
    }

    @Override
    public double getDistributionTolerance() {
        return 0.01;
    }

    @Override
    public Iterator<Map<String, Long>> getGeneratorImpl(GeneratorFactory generatorFactory) {
        Tuple3<Double, String, Iterator<Long>> t1 = Tuple.tuple3(1.0, "1",
                generatorFactory.constant(1l));
        Tuple3<Double, String, Iterator<Long>> t2 = Tuple.tuple3(2.0, "2",
                generatorFactory.constant(2l));
        Tuple3<Double, String, Iterator<Long>> t3 = Tuple.tuple3(1.0, "3",
                generatorFactory.constant(3l));

        ArrayList<Tuple3<Double, String, Iterator<Long>>> items = new ArrayList<Tuple3<Double, String, Iterator<Long>>>();
        items.add(t1);
        items.add(t2);
        items.add(t3);
        Integer amountToRetrieve = 2;
        Iterator<Map<String, Long>> generator = generatorFactory.weightedDiscreteMap(items,
                amountToRetrieve);
        return generator;
    }
}
