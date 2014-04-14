package com.ldbc.driver.runtime.metrics;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.concurrent.TimeUnit;

public class ContinuousMetricSnapshot {
    @JsonProperty(value = "name")
    private String name;
    @JsonProperty(value = "unit")
    private TimeUnit unit;
    @JsonProperty(value = "count")
    private long count;
    @JsonProperty(value = "mean")
    private double mean;
    @JsonProperty(value = "min")
    private long min;
    @JsonProperty(value = "max")
    private long max;
    @JsonProperty(value = "50th_percentile")
    private long percentile50;
    @JsonProperty(value = "90th_percentile")
    private long percentile90;
    @JsonProperty(value = "95th_percentile")
    private long percentile95;
    @JsonProperty(value = "99th_percentile")
    private long percentile99;

    private ContinuousMetricSnapshot() {
    }

    ContinuousMetricSnapshot(String name,
                             TimeUnit unit,
                             long count,
                             double mean,
                             long min,
                             long max,
                             long percentile50,
                             long percentile90,
                             long percentile95,
                             long percentile99) {
        this.name = name;
        this.unit = unit;
        this.count = count;
        this.mean = mean;
        this.min = min;
        this.max = max;
        this.percentile50 = percentile50;
        this.percentile90 = percentile90;
        this.percentile95 = percentile95;
        this.percentile99 = percentile99;
    }

    public String name() {
        return name;
    }

    public TimeUnit unit() {
        return unit;
    }

    public long count() {
        return count;
    }

    public double mean() {
        return mean;
    }

    public long min() {
        return min;
    }

    public long max() {
        return max;
    }

    public long percentile50() {
        return percentile50;
    }

    public long percentile90() {
        return percentile90;
    }

    public long percentile95() {
        return percentile95;
    }

    public long percentile99() {
        return percentile99;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ContinuousMetricSnapshot that = (ContinuousMetricSnapshot) o;

        if (count != that.count) return false;
        if (max != that.max) return false;
        if (Double.compare(that.mean, mean) != 0) return false;
        if (min != that.min) return false;
        if (percentile50 != that.percentile50) return false;
        if (percentile90 != that.percentile90) return false;
        if (percentile95 != that.percentile95) return false;
        if (percentile99 != that.percentile99) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (unit != that.unit) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = name != null ? name.hashCode() : 0;
        result = 31 * result + (unit != null ? unit.hashCode() : 0);
        result = 31 * result + (int) (count ^ (count >>> 32));
        temp = Double.doubleToLongBits(mean);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (min ^ (min >>> 32));
        result = 31 * result + (int) (max ^ (max >>> 32));
        result = 31 * result + (int) (percentile50 ^ (percentile50 >>> 32));
        result = 31 * result + (int) (percentile90 ^ (percentile90 >>> 32));
        result = 31 * result + (int) (percentile95 ^ (percentile95 >>> 32));
        result = 31 * result + (int) (percentile99 ^ (percentile99 >>> 32));
        return result;
    }
}
