package com.ldbc.driver.generator;

import com.ldbc.driver.util.Tuple.Tuple2;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.ArrayList;

public class DiscreteGenerator<GENERATE_TYPE> extends Generator<GENERATE_TYPE> {
    private final ArrayList<Tuple2<Double, GENERATE_TYPE>> itemProbabilities;
    private final double probabilitiesSum;
    private final RandomDataGenerator random;

    DiscreteGenerator(RandomDataGenerator random, Iterable<Tuple2<Double, GENERATE_TYPE>> itemProbabilities) {
        if (false == itemProbabilities.iterator().hasNext()) {
            throw new GeneratorException("DiscreteMultiGenerator cannot be empty");
        }

        this.random = random;
        this.itemProbabilities = new ArrayList<Tuple2<Double, GENERATE_TYPE>>();
        double sum = 0;
        for (Tuple2<Double, GENERATE_TYPE> item : itemProbabilities) {
            this.itemProbabilities.add(item);
            sum += item._1();
        }
        probabilitiesSum = sum;
    }

    @Override
    protected GENERATE_TYPE doNext() {
        double randomValue = random.nextUniform(0, 1);

        for (Tuple2<Double, GENERATE_TYPE> item : itemProbabilities) {
            if (randomValue < (item._1() / probabilitiesSum)) {
                return item._2();
            }
            randomValue = randomValue - (item._1() / probabilitiesSum);
        }

        String errMsg = String.format("randomValue=%s\nprobabilitiesSum=%s\nitems=%s\n", randomValue, probabilitiesSum,
                itemProbabilities.toString());
        throw new GeneratorException(String.format("Unexpected Error - failed to select next discrete element - should never get to this line\n%s", errMsg));
    }

    @Override
    public String toString() {
        return "DiscreteGenerator [items=" + itemProbabilities.toString() + "]";
    }
}
