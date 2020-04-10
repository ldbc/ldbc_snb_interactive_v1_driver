package com.ldbc.driver.client;

import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.LoggingService;

public class PrintHelpMode extends ClientMode
{
    private final ControlService controlService;
    private final LoggingService loggingService;


    public PrintHelpMode(ControlService controlService)
    {
        super(ClientModeType.PRINT_HELP);
        this.controlService = controlService;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor(getClass().getSimpleName());
    }

    @Override
    public void startExecutionAndAwaitCompletion() {
        loggingService.info(controlService.configuration().helpString());
    }

}
