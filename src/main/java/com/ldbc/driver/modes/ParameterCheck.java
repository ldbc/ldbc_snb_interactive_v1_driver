package com.ldbc.driver.modes;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;

import java.util.ArrayList;
import java.util.List;

import static com.ldbc.driver.modes.DriverModeType.*;
import static java.lang.String.format;

/**
 * This class provides a static method for checking if the required parameters are set for a given driver mode.
 */
public class ParameterCheck {

    public static void missingParameters(DriverConfiguration configuration, DriverModeType driverModeType) throws ClientException {

        List<String> missingParams = new ArrayList<>();

        if (null == configuration.getWorkloadClassName()) {
            missingParams.add(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG);
        }

        if (driverModeType == CREATE_VALIDATION_PARAMS ||
                driverModeType == VALIDATE_DATABASE ||
                driverModeType == EXECUTE_WORKLOAD) {
            if (null == configuration.getDbClassName()) {
                missingParams.add(ConsoleAndFileDriverConfiguration.DB_ARG);
            }
        }

        if (driverModeType == CREATE_VALIDATION_PARAMS ||
                driverModeType == CALCULATE_WORKLOAD_STATS ||
                driverModeType == EXECUTE_WORKLOAD) {
            if (null == configuration.getDbClassName()) {
                missingParams.add(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG);
            }
        }

        if (!missingParams.isEmpty()) {
            throw new ClientException(format("Missing required parameters: %s", missingParams.toString()));
        }

    }
}
