package com.ldbc.driver;

import com.ldbc.driver.modes.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

// TODO Validate Workload to work with short reads

public class Client {

    public static void main(String[] args) {
        ControlService controlService = null;
        boolean detailedStatus = false;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory(detailedStatus);
        LoggingService loggingService = loggingServiceFactory.loggingServiceFor(Client.class.getSimpleName());
        try {
            TimeSource systemTimeSource = new SystemTimeSource();
            ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(args);
            // TODO this method will not work with multiple processes - should come from controlService
            long workloadStartTimeAsMilli = systemTimeSource.nowAsMilli() + TimeUnit.SECONDS.toMillis(5);
            controlService = new LocalControlService(
                    workloadStartTimeAsMilli,
                    configuration,
                    loggingServiceFactory,
                    systemTimeSource);

            DriverModeType driverModeType = controlService.getConfiguration().getDriverMode();
            DriverMode driverMode = DriverModeFactory.buildDriverMode(driverModeType, controlService);
            driverMode.init();
            driverMode.startExecutionAndAwaitCompletion();

        } catch (DriverConfigurationException e) {
            String errMsg = format("Error parsing parameters: %s", e.getMessage());
            loggingService.info(errMsg);
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            loggingService.info("Client terminated unexpectedly\n" + ConcurrentErrorReporter.stackTraceToString(e));
            System.exit(1);
        } finally {
            if (null != controlService) {
                controlService.shutdown();
            }
        }
    }

}