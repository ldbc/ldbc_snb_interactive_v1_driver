package com.ldbc.driver.modes;

import com.ldbc.driver.ClientException;

public abstract class DriverMode
{
    private DriverModeType driverModeType;

    public DriverMode(DriverModeType driverModeType) {
        this.driverModeType = driverModeType;

    }

    public DriverModeType getDriverModeType() {
        return driverModeType;
    }

    public void init() throws ClientException {

    }

    public Object startExecutionAndAwaitCompletion() throws ClientException {
        return null;
    }
}
