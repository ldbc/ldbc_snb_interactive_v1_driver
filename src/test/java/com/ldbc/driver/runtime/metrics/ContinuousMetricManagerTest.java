package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ContinuousMetricManagerTest {

    // TODO look into this: hdrHistogram.getEstimatedFootprintInBytes()
// TODO   read highest durations from config instead of constants
// TODO   add tests to discover behavior when significant digits change. e.g., what happens if max value is high and actual measurements are low

    private final GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

    @Test
    public void shouldBehaveAsExpectedWithSimpleUsage() throws MetricsCollectionException {
        // Given
        String name = "name";
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        long highestExpectedValue = 3l;
        int numberOfSignificantDigits = 1;
        ContinuousMetricManager continuousMetricManager = new ContinuousMetricManager(name, timeUnit, highestExpectedValue, numberOfSignificantDigits);

        // When
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

        // Then
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

    @Test
    public void shouldBehaveAsExpectedWithHighNumberOfMeasurementsAndLargeRange() throws MetricsCollectionException {
        // Given
        String name = "name";
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        long highestExpectedValue = TimeUnit.MINUTES.toMillis(60);
        long lowestExpectedValue = TimeUnit.MINUTES.toMillis(1);
        long measurementCount = 10000000;
        int numberOfSignificantDigits = 5;
        ContinuousMetricManager continuousMetricManager = new ContinuousMetricManager(name, timeUnit, highestExpectedValue, numberOfSignificantDigits);

        // When
        Iterator<Long> measurements = gf.limit(gf.uniform(lowestExpectedValue, highestExpectedValue), measurementCount);
        while (measurements.hasNext()) {
            continuousMetricManager.addMeasurement(measurements.next());
        }

        // Then
        double expectedMean = ((highestExpectedValue - lowestExpectedValue) / 2) + lowestExpectedValue;
        long expectedPercentile50 = (((highestExpectedValue - lowestExpectedValue) / 100) * 50) + lowestExpectedValue;
        long expectedPercentile90 = (((highestExpectedValue - lowestExpectedValue) / 100) * 90) + lowestExpectedValue;
        long expectedPercentile95 = (((highestExpectedValue - lowestExpectedValue) / 100) * 95) + lowestExpectedValue;
        long expectedPercentile99 = (((highestExpectedValue - lowestExpectedValue) / 100) * 99) + lowestExpectedValue;
        ContinuousMetricSnapshot snapshot = continuousMetricManager.snapshot();
        assertThat(snapshot.name(), is(name));
        assertThat(snapshot.unit(), is(timeUnit));
        assertThat(snapshot.count(), is(measurementCount));
        assertThat(withinTolerance(snapshot.mean(), expectedMean, 500d), is(true));
        assertThat(withinTolerance(snapshot.percentile50(), expectedPercentile50, 500d), is(true));
        assertThat(withinTolerance(snapshot.percentile90(), expectedPercentile90, 500d), is(true));
        assertThat(withinTolerance(snapshot.percentile95(), expectedPercentile95, 500d), is(true));
        assertThat(withinTolerance(snapshot.percentile99(), expectedPercentile99, 500d), is(true));
    }

    private boolean withinTolerance(double actualValue, double expectedValue, double tolerance) {
        return (expectedValue - tolerance) <= actualValue && actualValue < expectedValue + tolerance;
    }

}
