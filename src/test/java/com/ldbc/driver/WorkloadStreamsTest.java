package com.ldbc.driver;

import com.google.common.collect.Lists;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class WorkloadStreamsTest {
    @Test
    public void shouldLimitCorrectly() {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        List<Operation<?>> stream0 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(0), Time.fromMilli(0), "0-1"),
                new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "0-2"),
                new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "0-3"),
                new TimedNamedOperation1(Time.fromMilli(6), Time.fromMilli(0), "0-4"),
                new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(0), "0-5")
        );

        List<Operation<?>> stream1 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(0), Time.fromMilli(0), "1-1"),
                new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "1-2"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "1-3"),
                new TimedNamedOperation1(Time.fromMilli(9), Time.fromMilli(0), "1-4")
        );

        List<Operation<?>> stream2 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "2-1"),
                new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "2-2"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "2-3"),
                new TimedNamedOperation1(Time.fromMilli(8), Time.fromMilli(0), "2-4"),
                new TimedNamedOperation1(Time.fromMilli(8), Time.fromMilli(0), "2-5"),
                new TimedNamedOperation1(Time.fromMilli(9), Time.fromMilli(0), "2-6")
        );

        List<Operation<?>> stream3 = Lists.newArrayList(
        );

        List<Operation<?>> stream4 = Lists.newArrayList(gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(Time.fromMilli(10), Duration.fromMilli(1)),
                                gf.constant(Time.fromMilli(0)),
                                gf.constant("4-x")
                        ),
                        1000000
                )
        );

        List<Iterator<Operation<?>>> streams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator(),
                stream4.iterator()
        );

        long k = 10;
        long[] kForIterator = WorkloadStreams.fromAmongAllRetrieveTopK(streams, k);

        List<Operation<?>> topK = Lists.newArrayList(
                gf.mergeSortOperationsByStartTime(
                        gf.limit(
                                stream0.iterator(),
                                kForIterator[0]
                        ),
                        gf.limit(
                                stream1.iterator(),
                                kForIterator[1]
                        ),
                        gf.limit(
                                stream2.iterator(),
                                kForIterator[2]
                        ),
                        gf.limit(
                                stream3.iterator(),
                                kForIterator[3]
                        ),
                        gf.limit(
                                stream4.iterator(),
                                kForIterator[4]
                        )
                )
        );

        for (Operation<?> operation : topK) {
            System.out.println(((TimedNamedOperation1) operation).name() + " " + operation.scheduledStartTime().asMilli());
        }

        assertThat((long) topK.size(), is(k));
        assertThat(((TimedNamedOperation1) topK.get(0)).name(), anyOf(equalTo("0-1"), equalTo("1-1")));
        assertThat(((TimedNamedOperation1) topK.get(1)).name(), anyOf(equalTo("0-1"), equalTo("1-1")));
        assertThat(((TimedNamedOperation1) topK.get(0)).name(), not(equalTo(((TimedNamedOperation1) topK.get(1)).name())));
        assertThat(((TimedNamedOperation1) topK.get(2)).name(), anyOf(equalTo("0-2"), equalTo("2-1")));
        assertThat(((TimedNamedOperation1) topK.get(3)).name(), anyOf(equalTo("0-2"), equalTo("2-1")));
        assertThat(((TimedNamedOperation1) topK.get(2)).name(), not(equalTo(((TimedNamedOperation1) topK.get(3)).name())));
        assertThat(((TimedNamedOperation1) topK.get(4)).name(), anyOf(equalTo("0-3")));
        assertThat(((TimedNamedOperation1) topK.get(5)).name(), anyOf(equalTo("1-2"), equalTo("2-2")));
        assertThat(((TimedNamedOperation1) topK.get(6)).name(), anyOf(equalTo("1-2"), equalTo("2-2")));
        assertThat(((TimedNamedOperation1) topK.get(5)).name(), not(equalTo(((TimedNamedOperation1) topK.get(6)).name())));
        assertThat(((TimedNamedOperation1) topK.get(7)).name(), anyOf(equalTo("1-3"), equalTo("2-3")));
        assertThat(((TimedNamedOperation1) topK.get(8)).name(), anyOf(equalTo("1-3"), equalTo("2-3")));
        assertThat(((TimedNamedOperation1) topK.get(7)).name(), not(equalTo(((TimedNamedOperation1) topK.get(8)).name())));
        assertThat(((TimedNamedOperation1) topK.get(9)).name(), anyOf(equalTo("0-4")));
    }
}
