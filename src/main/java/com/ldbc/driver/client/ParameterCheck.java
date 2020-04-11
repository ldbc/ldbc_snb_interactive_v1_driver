package com.ldbc.driver.client;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;

import java.util.ArrayList;
import java.util.List;

import static com.ldbc.driver.client.ClientModeType.*;
import static java.lang.String.format;

/**
 * This class provides a static method for checking if the required parameters are set for a given driver mode.
 */
public class ParameterCheck {

    public static void missingParameters(DriverConfiguration configuration, ClientModeType clientModeType) throws ClientException {

        List<String> missingParams = new ArrayList<>();

        if (null == configuration.workloadClassName()) {
            missingParams.add(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG);
        }

        if (clientModeType == CREATE_VALIDATION_PARAMS ||
                clientModeType == VALIDATE_DATABASE ||
                clientModeType == EXECUTE_WORKLOAD) {
            if (null == configuration.dbClassName()) {
                missingParams.add(ConsoleAndFileDriverConfiguration.DB_ARG);
            }
        }

        if (clientModeType == CREATE_VALIDATION_PARAMS ||
                clientModeType == CALCULATE_WORKLOAD_STATS ||
                clientModeType == EXECUTE_WORKLOAD) {
            if (null == configuration.dbClassName()) {
                missingParams.add(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG);
            }
        }

        if (!missingParams.isEmpty()) {
            throw new ClientException(format("Missing required parameters: %s", missingParams.toString()));
        }

    }
}
