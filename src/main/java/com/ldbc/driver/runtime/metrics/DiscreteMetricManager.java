package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Bucket.DiscreteBucket;
import com.ldbc.driver.util.Histogram;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class DiscreteMetricManager {
    private final Histogram<Long, Integer> measurements;
    private final String name;
    private final String unit;
    private long measurementMin = Long.MAX_VALUE;
    private long measurementMax = Long.MIN_VALUE;

    public DiscreteMetricManager(String name, String unit) {
        this.measurements = new Histogram<>(0);
        this.name = name;
        this.unit = unit;
    }

    public void addMeasurement(long value) {
        measurements.incOrCreateBucket(DiscreteBucket.create(value), 1);
        measurementMin = (value < measurementMin) ? value : measurementMin;
        measurementMax = (value > measurementMax) ? value : measurementMax;
    }

    public DiscreteMetricSnapshot snapshot() {
        return new DiscreteMetricSnapshot(name, unit, count(), allValues());
    }

    private long count() {
        return measurements.sumOfAllBucketValues();
    }

    private Map<Long, Long> allValues() {
        Map<Long, Long> allValuesMap = new HashMap<Long, Long>();
        for (Entry<Bucket<Long>, Integer> entry : measurements.getAllBuckets()) {
            long resultCode = ((DiscreteBucket<Long>) entry.getKey()).getId();
            long resultCodeCount = entry.getValue();
            allValuesMap.put(resultCode, resultCodeCount);
        }
        return allValuesMap;
    }
}
