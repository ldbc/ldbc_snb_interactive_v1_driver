package com.ldbc.driver.runtime.scheduler;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.List;

// TODO test
public class UniformWindowedScheduler implements Scheduler<List<OperationHandler<?>>, Window.OperationHandlerTimeRangeWindow> {
    @Override
    public List<OperationHandler<?>> schedule(Window.OperationHandlerTimeRangeWindow handlersWindow) {
        return assignUniformlyDistributedStartTimes(
                handlersWindow.contents(),
                handlersWindow.windowStartTimeInclusive(),
                handlersWindow.windowEndTimeExclusive());
    }

    private List<OperationHandler<?>> assignUniformlyDistributedStartTimes(List<OperationHandler<?>> operationHandlersInWindow,
                                                                           Time windowStartTime,
                                                                           Time windowEndTime) {
        int handlerCount = operationHandlersInWindow.size();
        Duration windowSize = windowEndTime.greaterBy(windowStartTime);
        Duration handlerInterleave = Duration.fromMilli(windowSize.asMilli() / handlerCount);
        for (int i = 0; i < operationHandlersInWindow.size(); i++) {
            Duration durationFromWindowStartTime = Duration.fromMilli(handlerInterleave.asMilli() * i);
            Time uniformHandlerStartTime = windowStartTime.plus(durationFromWindowStartTime);
            operationHandlersInWindow.get(i).operation().setScheduledStartTime(uniformHandlerStartTime);
        }
        return operationHandlersInWindow;
    }
}