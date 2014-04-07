package com.ldbc.driver.generator;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TimeMappingGeneratorTests {
    private final long RANDOM_SEED = 42;
    private GeneratorFactory generators = null;

    @Before
    public final void initGeneratorFactory() {
        generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    @Test
    public void shouldOffset() {
        // Given
        Function<Integer, Operation<?>> msToOperationFun = new Function<Integer, Operation<?>>() {
            @Override
            public Operation<?> apply(Integer timeMs) {
                Operation<?> operation = new Operation<Object>() {
                };
                operation.setScheduledStartTime(Time.fromMilli(timeMs));
                return operation;
            }
        };
        List<Operation<?>> operations = ImmutableList.copyOf(Iterators.transform(generators.boundedIncrementing(0, 100, 1000), msToOperationFun));
        assertThat(operations.size(), is(11));
        assertThat(operations.get(0).scheduledStartTime(), equalTo(Time.fromMilli(0)));
        assertThat(operations.get(10).scheduledStartTime(), equalTo(Time.fromMilli(1000)));
        Time newStartTime = Time.fromMilli(500);

        // When
        List<Operation<?>> offsetOperations = ImmutableList.copyOf(generators.timeOffset(operations.iterator(), newStartTime));

        // Then
        assertThat(offsetOperations.size(), is(11));
        assertThat(offsetOperations.get(0).scheduledStartTime(), equalTo(newStartTime));
        assertThat(offsetOperations.get(1).scheduledStartTime(), equalTo(Time.fromMilli(600)));
        assertThat(offsetOperations.get(2).scheduledStartTime(), equalTo(Time.fromMilli(700)));
        assertThat(offsetOperations.get(3).scheduledStartTime(), equalTo(Time.fromMilli(800)));
        assertThat(offsetOperations.get(4).scheduledStartTime(), equalTo(Time.fromMilli(900)));
        assertThat(offsetOperations.get(5).scheduledStartTime(), equalTo(Time.fromMilli(1000)));
        assertThat(offsetOperations.get(6).scheduledStartTime(), equalTo(Time.fromMilli(1100)));
        assertThat(offsetOperations.get(7).scheduledStartTime(), equalTo(Time.fromMilli(1200)));
        assertThat(offsetOperations.get(8).scheduledStartTime(), equalTo(Time.fromMilli(1300)));
        assertThat(offsetOperations.get(9).scheduledStartTime(), equalTo(Time.fromMilli(1400)));
        assertThat(offsetOperations.get(10).scheduledStartTime(), equalTo(Time.fromMilli(1500)));
    }

    @Test
    public void shouldOffsetAndCompress() {
        // Given
        Function<Integer, Operation<?>> msToOperationFun = new Function<Integer, Operation<?>>() {
            @Override
            public Operation<?> apply(Integer timeMs) {
                Operation<?> operation = new Operation<Object>() {
                };
                operation.setScheduledStartTime(Time.fromMilli(timeMs));
                return operation;
            }
        };
        List<Operation<?>> operations = ImmutableList.copyOf(Iterators.transform(generators.boundedIncrementing(0, 100, 1000), msToOperationFun));
        assertThat(operations.size(), is(11));
        assertThat(operations.get(0).scheduledStartTime(), equalTo(Time.fromMilli(0)));
        assertThat(operations.get(10).scheduledStartTime(), equalTo(Time.fromMilli(1000)));
        Time newStartTime = Time.fromMilli(500);
        Double compressionRatio = 0.2;

        // When
        List<Operation<?>> offsetAndCompressedOperations = ImmutableList.copyOf(generators.timeOffsetAndCompress(operations.iterator(), newStartTime, compressionRatio));

        // Then
        assertThat(offsetAndCompressedOperations.size(), is(11));
        assertThat(offsetAndCompressedOperations.get(0).scheduledStartTime(), equalTo(newStartTime));
        assertThat(offsetAndCompressedOperations.get(1).scheduledStartTime(), equalTo(Time.fromMilli(520)));
        assertThat(offsetAndCompressedOperations.get(2).scheduledStartTime(), equalTo(Time.fromMilli(540)));
        assertThat(offsetAndCompressedOperations.get(3).scheduledStartTime(), equalTo(Time.fromMilli(560)));
        assertThat(offsetAndCompressedOperations.get(4).scheduledStartTime(), equalTo(Time.fromMilli(580)));
        assertThat(offsetAndCompressedOperations.get(5).scheduledStartTime(), equalTo(Time.fromMilli(600)));
        assertThat(offsetAndCompressedOperations.get(6).scheduledStartTime(), equalTo(Time.fromMilli(620)));
        assertThat(offsetAndCompressedOperations.get(7).scheduledStartTime(), equalTo(Time.fromMilli(640)));
        assertThat(offsetAndCompressedOperations.get(8).scheduledStartTime(), equalTo(Time.fromMilli(660)));
        assertThat(offsetAndCompressedOperations.get(9).scheduledStartTime(), equalTo(Time.fromMilli(680)));
        assertThat(offsetAndCompressedOperations.get(10).scheduledStartTime(), equalTo(Time.fromMilli(700)));
    }
}
