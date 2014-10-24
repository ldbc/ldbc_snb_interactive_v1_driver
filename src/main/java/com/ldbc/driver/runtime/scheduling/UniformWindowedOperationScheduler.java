package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.temporal.TemporalUtil;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class UniformWindowedOperationScheduler implements Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> {
    private final TemporalUtil temporalUtil = new TemporalUtil();

    @Override
    public List<Operation<?>> schedule(Window.OperationTimeRangeWindow window) {
        return assignUniformlyDistributedStartTimes(
                window.contents(),
                window.windowStartTimeAsMilliInclusive(),
                window.windowEndTimeAsMilliExclusive());
    }

    private List<Operation<?>> assignUniformlyDistributedStartTimes(List<Operation<?>> operationsInWindow,
                                                                    long windowStartTimeAsMilli,
                                                                    long windowEndTimeAsMilli) {
        if (operationsInWindow.isEmpty())
            return operationsInWindow;

        double operationCount = operationsInWindow.size();
        long windowSizeAsMilli = windowEndTimeAsMilli - windowStartTimeAsMilli;
        double operationInterleaveAsNano = temporalUtil.convert(windowSizeAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) / operationCount;
        for (int i = 0; i < operationsInWindow.size(); i++) {
            long durationFromWindowStartTimeAsMilli = temporalUtil.convert(Math.round(Math.floor(operationInterleaveAsNano * i)), TimeUnit.NANOSECONDS, TimeUnit.MILLISECONDS);
            long uniformOperationStartTimeAsMilli = windowStartTimeAsMilli + durationFromWindowStartTimeAsMilli;
            operationsInWindow.get(i).setScheduledStartTimeAsMilli(uniformOperationStartTimeAsMilli);
        }
        return operationsInWindow;
    }
}