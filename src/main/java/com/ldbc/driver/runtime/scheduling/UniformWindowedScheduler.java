package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.List;

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
        if (operationHandlersInWindow.isEmpty())
            return operationHandlersInWindow;

        double handlerCount = operationHandlersInWindow.size();
        Duration windowSize = windowEndTime.greaterBy(windowStartTime);
        double handlerInterleaveAsNano = windowSize.asNano() / handlerCount;
        for (int i = 0; i < operationHandlersInWindow.size(); i++) {
            Duration durationFromWindowStartTime = Duration.fromNano(Math.round(Math.floor(handlerInterleaveAsNano * i)));
            Time uniformHandlerStartTime = windowStartTime.plus(durationFromWindowStartTime);
            operationHandlersInWindow.get(i).operation().setScheduledStartTime(uniformHandlerStartTime);
        }
        return operationHandlersInWindow;
    }
}