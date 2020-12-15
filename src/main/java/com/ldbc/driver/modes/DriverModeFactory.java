package com.ldbc.driver.modes;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;

import static com.ldbc.driver.modes.ParameterCheck.*;

public class DriverModeFactory {

    private static final long RANDOM_SEED = 42;

    public static DriverMode buildDriverMode(DriverModeType driverModeType, ControlService controlService) throws ClientException {
        DriverMode driverMode;
        missingParameters(controlService.getConfiguration(), driverModeType);
        switch (driverModeType) {
            case MISSING:
                throw new IllegalStateException("Driver mode missing");
            case CREATE_VALIDATION_PARAMS:
                driverMode = new CreateValidationParamsMode(controlService, RANDOM_SEED);
                break;
            case VALIDATE_DATABASE:
                driverMode = new ValidateDatabaseMode(controlService);
                break;
            case CALCULATE_WORKLOAD_STATS:
                driverMode = new CalculateWorkloadStatisticsMode(controlService, RANDOM_SEED);
                break;
            case EXECUTE_WORKLOAD:
                TimeSource systemTimeSource = new SystemTimeSource();
                driverMode = new ExecuteWorkloadMode(controlService, systemTimeSource, RANDOM_SEED);
                break;
            case PRINT_HELP:
                driverMode = new PrintHelpMode(controlService);
                break;
            default:
                throw new IllegalStateException("Specified mode not supported.");
        }
        return driverMode;
    }
}
