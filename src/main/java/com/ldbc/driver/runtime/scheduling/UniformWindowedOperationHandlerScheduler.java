package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.temporal.TemporalUtil;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class UniformWindowedOperationHandlerScheduler implements Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> {
    private final TemporalUtil temporalUtil = new TemporalUtil();

    @Override
    public List<OperationHandler<?>> schedule(Window.OperationHandlerTimeRangeWindow window) {
        return assignUniformlyDistributedStartTimes(
                window.contents(),
                window.windowStartTimeAsMilliInclusive(),
                window.windowEndTimeAsMilliExclusive());
    }

    private List<OperationHandler<?>> assignUniformlyDistributedStartTimes(List<OperationHandler<?>> operationHandlersInWindow,
                                                                           long windowStartTimeAsMilli,
                                                                           long windowEndTimeAsMilli) {
        if (operationHandlersInWindow.isEmpty())
            return operationHandlersInWindow;

        double handlerCount = operationHandlersInWindow.size();
        long windowSizeAsMilli = windowEndTimeAsMilli - windowStartTimeAsMilli;
        double handlerInterleaveAsNano = temporalUtil.convert(windowSizeAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) / handlerCount;
        for (int i = 0; i < operationHandlersInWindow.size(); i++) {
            long durationFromWindowStartTimeAsMilli = temporalUtil.convert(Math.round(Math.floor(handlerInterleaveAsNano * i)), TimeUnit.NANOSECONDS, TimeUnit.MILLISECONDS);
            long uniformHandlerStartTimeAsMilli = windowStartTimeAsMilli + durationFromWindowStartTimeAsMilli;
            operationHandlersInWindow.get(i).operation().setScheduledStartTimeAsMilli(uniformHandlerStartTimeAsMilli);
        }
        return operationHandlersInWindow;
    }
}