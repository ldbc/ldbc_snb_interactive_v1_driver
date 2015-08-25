package com.ldbc.driver.client;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.LoggingService;

public class PrintHelpMode implements ClientMode<Object>
{
    private final ControlService controlService;
    private final LoggingService loggingService;

    public PrintHelpMode( ControlService controlService )
    {
        this.controlService = controlService;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor( getClass().getSimpleName() );
    }

    @Override
    public void init() throws ClientException
    {
    }

    @Override
    public Object startExecutionAndAwaitCompletion() throws ClientException
    {
        loggingService.info( controlService.configuration().helpString() );
        return null;
    }

}
