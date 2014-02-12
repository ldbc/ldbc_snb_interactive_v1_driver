package com.ldbc.driver.util;

import com.google.common.collect.Range;
import com.ldbc.driver.util.Bucket.NumberRangeBucket;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class HistogramTest {
    @Test
    public void makeEqualBucketRangesTest() {
        // Given
        double min = 0;
        double max = 10;
        int bucketCount = 5;

        // When
        List<Bucket<Integer>> buckets = Histogram.makeBucketsOfEqualRange(min, max, bucketCount, Integer.class);

        // Then
        assertEquals(bucketCount, buckets.size());
    }

    @Test
    public void createEmpty() {
        // Given

        // When
        Histogram<Integer, Integer> h = new Histogram<Integer, Integer>(0);

        // Then
        assertEquals(0, h.getBucketCount());
    }

    @Test
    public void importSequenceTest() {
        // Given
        Bucket<Integer> bucket1 = new NumberRangeBucket<Integer>(Range.closedOpen(0d, 2d));
        Bucket<Integer> bucket2 = new NumberRangeBucket<Integer>(Range.closedOpen(2d, 4d));
        Bucket<Integer> bucket3 = new NumberRangeBucket<Integer>(Range.closedOpen(4d, 6d));
        Bucket<Integer> bucket4 = new NumberRangeBucket<Integer>(Range.closed(6d, 8d));
        Histogram<Integer, Integer> histogram = new Histogram<Integer, Integer>(0);
        histogram.addBucket(bucket1); // 3
        histogram.addBucket(bucket2); // 2
        histogram.addBucket(bucket3); // 0
        histogram.addBucket(bucket4); // 1

        Integer[] sequenceArray = new Integer[]{0, 1, 1, 2, 2, 3, 8};
        List<Integer> sequence = Arrays.asList(sequenceArray);

        assertEquals(0, (int) histogram.getBucketValue(bucket1));
        assertEquals(0, (int) histogram.getBucketValue(bucket2));
        assertEquals(0, (int) histogram.getBucketValue(bucket3));
        assertEquals(0, (int) histogram.getBucketValue(bucket4));

        // When
        histogram.importValueSequence(sequence);

        // Then
        assertEquals(3, (int) histogram.getBucketValue(bucket1));
        assertEquals(3, (int) histogram.getBucketValue(bucket2));
        assertEquals(0, (int) histogram.getBucketValue(bucket3));
        assertEquals(1, (int) histogram.getBucketValue(bucket4));

        assertEquals(4, histogram.getBucketCount());
    }

    @Test
    public void addBucketsTest() {
        // Given
        Histogram<Integer, Integer> histogram = new Histogram<Integer, Integer>(2);
        List<Bucket<Integer>> buckets = new ArrayList<Bucket<Integer>>();
        buckets.add(new NumberRangeBucket<Integer>(Range.closedOpen(0d, 2d)));
        buckets.add(new NumberRangeBucket<Integer>(Range.closedOpen(2d, 4d)));
        buckets.add(new NumberRangeBucket<Integer>(Range.closedOpen(4d, 6d)));
        buckets.add(new NumberRangeBucket<Integer>(Range.closed(6d, 8d)));
        assertEquals(0, histogram.getBucketCount());

        // When
        histogram.addBuckets(buckets);

        // Then
        assertEquals(4, histogram.getBucketCount());
        for (Bucket<Integer> bucket : buckets) {
            assertEquals(2, (int) histogram.getBucketValue(bucket));
        }
    }

    @Test
    public void addThenGetBucketTest() {
        // Given
        Histogram<Integer, Integer> histogram = new Histogram<Integer, Integer>(5);

        assertEquals(0, histogram.getBucketCount());

        // When
        histogram.addBucket(new NumberRangeBucket<Integer>(Range.closedOpen(0d, 2d)));

        // Then
        assertEquals(1, histogram.getBucketCount());
        assertEquals(5, (int) histogram.getBucketValue(new NumberRangeBucket<Integer>(Range.closedOpen(0d, 2d))));
    }

    @Test
    public void setAllBucketValuesTest() {
        // Given
        Histogram<Integer, Integer> histogram = new Histogram<Integer, Integer>(2);
        List<Bucket<Integer>> buckets = new ArrayList<Bucket<Integer>>();
        buckets.add(new NumberRangeBucket<Integer>(Range.closedOpen(0d, 2d)));
        buckets.add(new NumberRangeBucket<Integer>(Range.closedOpen(2d, 4d)));
        buckets.add(new NumberRangeBucket<Integer>(Range.closedOpen(4d, 6d)));
        buckets.add(new NumberRangeBucket<Integer>(Range.closed(6d, 8d)));
        histogram.addBuckets(buckets);

        for (Bucket<Integer> bucket : buckets) {
            assertEquals(2, (int) histogram.getBucketValue(bucket));
        }

        // When
        histogram.setAllBucketValues(9);

        // Then
        assertEquals(4, histogram.getBucketCount());
        for (Bucket<Integer> bucket : buckets) {
            assertEquals(9, (int) histogram.getBucketValue(bucket));
        }
    }

    @Test
    public void setBucketValuesTest() {
        // Given
        Histogram<Integer, Integer> histogram = new Histogram<Integer, Integer>(2);
        List<Bucket<Integer>> buckets = new ArrayList<Bucket<Integer>>();
        buckets.add(new NumberRangeBucket<Integer>(Range.closedOpen(0d, 2d)));
        buckets.add(new NumberRangeBucket<Integer>(Range.closedOpen(2d, 4d)));
        buckets.add(new NumberRangeBucket<Integer>(Range.closedOpen(4d, 6d)));
        buckets.add(new NumberRangeBucket<Integer>(Range.closed(6d, 8d)));
        histogram.addBuckets(buckets, 2);

        for (Bucket<Integer> bucket : buckets) {
            assertEquals(2, (int) histogram.getBucketValue(bucket));
        }

        // When
        histogram.setBucketValues(buckets, 9);

        // Then
        assertEquals(4, histogram.getBucketCount());
        for (Bucket<Integer> bucket : buckets) {
            assertEquals(9, (int) histogram.getBucketValue(bucket));
        }
    }

    @Test
    public void incBucketValueTest() {
        // Given
        Histogram<Integer, Integer> histogram = new Histogram<Integer, Integer>(5);

        assertEquals(0, histogram.getBucketCount());

        Bucket<Integer> bucket = new NumberRangeBucket<Integer>(Range.closedOpen(0d, 2d));
        histogram.addBucket(bucket);

        assertEquals(1, histogram.getBucketCount());
        assertEquals(5, (int) histogram.getBucketValue(bucket));

        // When
        histogram.incBucketValue(bucket, 1);

        // Then
        assertEquals(1, histogram.getBucketCount());
        assertEquals(6, (int) histogram.getBucketValue(bucket));
    }

    @Test
    public void toPercentageValuesTest() {
        // Given
        Bucket<Integer> bucket1 = new NumberRangeBucket<Integer>(Range.closedOpen(0d, 2d));
        Bucket<Integer> bucket2 = new NumberRangeBucket<Integer>(Range.closedOpen(2d, 4d));
        Bucket<Integer> bucket3 = new NumberRangeBucket<Integer>(Range.closed(4d, 6d));
        Histogram<Integer, Integer> histogramAbsolute = new Histogram<Integer, Integer>(0);
        histogramAbsolute.addBucket(bucket1, 1);
        histogramAbsolute.addBucket(bucket2, 1);
        histogramAbsolute.addBucket(bucket3, 2);

        assertEquals(1, (int) histogramAbsolute.getBucketValue(bucket1));
        assertEquals(1, (int) histogramAbsolute.getBucketValue(bucket2));
        assertEquals(2, (int) histogramAbsolute.getBucketValue(bucket3));

        // When
        Histogram<Integer, Double> histogramPercentage = histogramAbsolute.toPercentageValues();

        // Then
        assertEquals(1, (int) histogramAbsolute.getBucketValue(bucket1));
        assertEquals(1, (int) histogramAbsolute.getBucketValue(bucket2));
        assertEquals(2, (int) histogramAbsolute.getBucketValue(bucket3));

        assertEquals((Double) 0.25, histogramPercentage.getBucketValue(bucket1));
        assertEquals((Double) 0.25, histogramPercentage.getBucketValue(bucket2));
        assertEquals((Double) 0.5, histogramPercentage.getBucketValue(bucket3));
    }

    @Test
    public void equalsWithinToleranceTest() {
        // Given
        Bucket<Integer> bucket1 = new NumberRangeBucket<Integer>(Range.closedOpen(0d, 2d));
        Bucket<Integer> bucket2 = new NumberRangeBucket<Integer>(Range.closedOpen(2d, 4d));
        Histogram<Integer, Double> histogram1 = new Histogram<Integer, Double>(0d);
        histogram1.addBucket(bucket1, 0.5);
        histogram1.addBucket(bucket2, 0.2);
        Histogram<Integer, Double> histogram2 = new Histogram<Integer, Double>(0d);
        histogram2.addBucket(bucket1, 0.6);
        histogram2.addBucket(bucket2, 0.2);

        // When
        assertThat(Histogram.equalsWithinTolerance(histogram1, histogram2, 0.1), is(true));
        assertThat(Histogram.equalsWithinTolerance(histogram1, histogram2, 0.05), is(false));
        assertThat(Histogram.equalsWithinTolerance(histogram2, histogram1, 0.1), is(true));
        assertThat(Histogram.equalsWithinTolerance(histogram2, histogram1, 0.05), is(false));
    }

}
