package com.ldbc.driver.control;

public class DriverConfigurationException extends Exception
{
    private static final long serialVersionUID = 8844396756042772132L;

    public DriverConfigurationException(String message)
    {
        super( message );
    }

    public DriverConfigurationException()
    {
        super();
    }

    public DriverConfigurationException(String message, Throwable cause)
    {
        super( message, cause );
    }

    public DriverConfigurationException(Throwable cause)
    {
        super( cause );
    }
}
