package com.ldbc.driver.modes;

import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.modes.DriverMode;
import com.ldbc.driver.modes.DriverModeType;

public class PrintHelpMode extends DriverMode
{
    private final ControlService controlService;
    private final LoggingService loggingService;


    public PrintHelpMode(ControlService controlService)
    {
        super(DriverModeType.PRINT_HELP);
        this.controlService = controlService;
        this.loggingService = controlService.getLoggingServiceFactory().loggingServiceFor(getClass().getSimpleName());
    }

    @Override
    public Object startExecutionAndAwaitCompletion() {
        loggingService.info(controlService.getConfiguration().helpString());
        return null;
    }

}
