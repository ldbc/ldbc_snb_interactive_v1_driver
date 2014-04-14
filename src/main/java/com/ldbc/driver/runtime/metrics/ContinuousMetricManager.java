package com.ldbc.driver.runtime.metrics;

import org.HdrHistogram.Histogram;

import java.util.concurrent.TimeUnit;


public class ContinuousMetricManager {
    private final Histogram histogram;
    private final String name;
    private final TimeUnit unit;

    public ContinuousMetricManager(String name, TimeUnit unit, long highestExpectedValue, int numberOfSignificantDigits) {
        histogram = new Histogram(highestExpectedValue, numberOfSignificantDigits);
        this.name = name;
        this.unit = unit;
    }

    public void addMeasurement(long value) throws MetricsCollectionException {
        try {
            histogram.recordValue(value);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new MetricsCollectionException(String.format("Error encountered adding measurement [%s]", value), e.getCause());
        }
    }

    public ContinuousMetricSnapshot snapshot() {
        return new ContinuousMetricSnapshot(name, unit, count(), mean(), min(), max(), percentile50(), percentile90(), percentile95(), percentile99());
    }

    private long count() {
        return histogram.getHistogramData().getTotalCount();
    }

    private double mean() {
        return histogram.getHistogramData().getMean();
    }

    private long min() {
        return histogram.getHistogramData().getMinValue();
    }

    private long max() {
        return histogram.getHistogramData().getMaxValue();
    }

    private long percentile50() {
        return histogram.getHistogramData().getValueAtPercentile(50);
    }

    private long percentile90() {
        return histogram.getHistogramData().getValueAtPercentile(90);
    }

    private long percentile95() {
        return histogram.getHistogramData().getValueAtPercentile(95);
    }

    private long percentile99() {
        return histogram.getHistogramData().getValueAtPercentile(99);
    }
}
