package org.ldbcouncil.snb.driver.validation;
/**
 * ResultsLogValidationTolerances.java
 * 
 */


public class ResultsLogValidationTolerances
{
    private final long excessiveDelayThresholdAsMilli;
    private final long toleratedExcessiveDelayCount;
    private final double toleratedExcessiveDelayCountPercentage;

    public ResultsLogValidationTolerances(
        long excessiveDelayThresholdAsMilli,
        long toleratedExcessiveDelayCount,
        double toleratedExcessiveDelayCountPercentage)
    {
        this.excessiveDelayThresholdAsMilli = excessiveDelayThresholdAsMilli;
        this.toleratedExcessiveDelayCount = toleratedExcessiveDelayCount;
        this.toleratedExcessiveDelayCountPercentage = toleratedExcessiveDelayCountPercentage;
    }

    public long excessiveDelayThresholdAsMilli()
    {
        return excessiveDelayThresholdAsMilli;
    }

    public long toleratedExcessiveDelayCount()
    {
        return toleratedExcessiveDelayCount;
    }

    public double toleratedExcessiveDelayCountPercentage()
    {
        return toleratedExcessiveDelayCountPercentage;
    }
}
