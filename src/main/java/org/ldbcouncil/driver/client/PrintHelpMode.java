package org.ldbcouncil.driver.client;

import org.ldbcouncil.driver.ClientException;
import org.ldbcouncil.driver.control.ControlService;
import org.ldbcouncil.driver.control.LoggingService;

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
