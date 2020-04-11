package com.ldbc.driver.client;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;

import static com.ldbc.driver.client.ParameterCheck.*;

public class ClientModeFactory {

    private static final long RANDOM_SEED = 42;

    public static ClientMode buildClientMode(ClientModeType clientModeType, ControlService controlService) throws ClientException {
        ClientMode clientMode;
        missingParameters(controlService.configuration(),clientModeType);
        switch (clientModeType) {
            case CREATE_VALIDATION_PARAMS:
                clientMode = new CreateValidationParamsMode(controlService, RANDOM_SEED);
                break;
            case VALIDATE_DATABASE:
                clientMode = new ValidateDatabaseMode(controlService);
                break;
            case CALCULATE_WORKLOAD_STATS:
                clientMode = new CalculateWorkloadStatisticsMode(controlService, RANDOM_SEED);
                break;
            case EXECUTE_WORKLOAD:
                TimeSource systemTimeSource = new SystemTimeSource();
                clientMode = new ExecuteWorkloadMode(controlService, systemTimeSource, RANDOM_SEED);
                break;
            default:
                clientMode = new PrintHelpMode(controlService);
                break;
        }
        return clientMode;
    }
}
