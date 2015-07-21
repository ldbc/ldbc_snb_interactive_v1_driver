package com.ldbc.driver.client;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.validation.ClassNameWorkloadFactory;
import com.ldbc.driver.validation.WorkloadFactory;
import com.ldbc.driver.validation.WorkloadValidationResult;
import com.ldbc.driver.validation.WorkloadValidator;

public class ValidateWorkloadMode implements ClientMode<WorkloadValidationResult>
{
    private final ControlService controlService;
    private final WorkloadFactory workloadFactory;
    private final LoggingService loggingService;

    public ValidateWorkloadMode( ControlService controlService ) throws ClientException
    {
        this.controlService = controlService;
        this.workloadFactory = new ClassNameWorkloadFactory( controlService.configuration().workloadClassName() );
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor( getClass().getSimpleName() );
    }

    @Override
    public void init() throws ClientException
    {
        loggingService.info( "Driver Configuration" );
        loggingService.info( controlService.toString() );
    }

    @Override
    public WorkloadValidationResult startExecutionAndAwaitCompletion() throws ClientException
    {
        loggingService
                .info( String.format( "Validating workload: %s", controlService.configuration().workloadClassName() ) );
        WorkloadValidator workloadValidator = new WorkloadValidator();
        WorkloadValidationResult workloadValidationResult = workloadValidator.validate(
                workloadFactory,
                controlService.configuration(),
                controlService.loggingServiceFactory()
        );
        if ( workloadValidationResult.isSuccessful() )
        {
            loggingService.info( "Workload Validation Result: PASS" );
        }
        else
        {
            loggingService.info(
                    String.format( "Workload Validation Result: FAIL\n%s", workloadValidationResult.errorMessage() )
            );
        }
        return workloadValidationResult;
    }
}
