package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.Window;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.List;

public class UniformWindowedOperationScheduler implements Scheduler<List<Operation<?>>, Window.OperationTimeRangeWindow> {
    @Override
    public List<Operation<?>> schedule(Window.OperationTimeRangeWindow window) {
        return assignUniformlyDistributedStartTimes(
                window.contents(),
                window.windowStartTimeInclusive(),
                window.windowEndTimeExclusive());
    }

    private List<Operation<?>> assignUniformlyDistributedStartTimes(List<Operation<?>> operationsInWindow,
                                                                    Time windowStartTime,
                                                                    Time windowEndTime) {
        if (operationsInWindow.isEmpty())
            return operationsInWindow;

        double operationCount = operationsInWindow.size();
        Duration windowSize = windowEndTime.durationGreaterThan(windowStartTime);
        double operationInterleaveAsNano = windowSize.asNano() / operationCount;
        for (int i = 0; i < operationsInWindow.size(); i++) {
            Duration durationFromWindowStartTime = Duration.fromNano(Math.round(Math.floor(operationInterleaveAsNano * i)));
            Time uniformOperationStartTime = windowStartTime.plus(durationFromWindowStartTime);
            operationsInWindow.get(i).setScheduledStartTimeAsMilli(uniformOperationStartTime);
        }
        return operationsInWindow;
    }
}