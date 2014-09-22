package com.ldbc.driver.runtime.metrics;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ContinuousMetricManagerTest {
    @Test
    public void shouldBehaveAsExpectedWithSimpleUsage() throws MetricsCollectionException {
        String name = "name";
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        long highestExpectedValue = 3;
        int numberOfSignificantDigits = 1;
        ContinuousMetricManager continuousMetricManager = new ContinuousMetricManager(name, timeUnit, highestExpectedValue, numberOfSignificantDigits);

        continuousMetricManager.addMeasurement(0l);
        continuousMetricManager.addMeasurement(0l);
        continuousMetricManager.addMeasurement(0l);
        continuousMetricManager.addMeasurement(0l);
        continuousMetricManager.addMeasurement(0l);
        continuousMetricManager.addMeasurement(0l);
        continuousMetricManager.addMeasurement(0l);
        continuousMetricManager.addMeasurement(1l);
        continuousMetricManager.addMeasurement(2l);
        continuousMetricManager.addMeasurement(3l);

        ContinuousMetricSnapshot snapshot = continuousMetricManager.snapshot();
        assertThat(snapshot.name(), is(name));
        assertThat(snapshot.unit(), is(timeUnit));
        assertThat(snapshot.count(), is(10l));
        assertThat(snapshot.min(), is(0l));
        assertThat(snapshot.max(), is(3l));
        assertThat(snapshot.mean(), is(0.6d));
        assertThat(snapshot.percentile50(), is(0l));
        assertThat(snapshot.percentile90(), is(2l));
        assertThat(snapshot.percentile95(), is(3l));
        assertThat(snapshot.percentile99(), is(3l));
    }

// TODO   read highest durations from config instead of constants
// TODO   add tests with large amount of inputs;
// TODO   add tests with out of range inputs;
// TODO   add tests to discover behavior when significant digits changes;
}
