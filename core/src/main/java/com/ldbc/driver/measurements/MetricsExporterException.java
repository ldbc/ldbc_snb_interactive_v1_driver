package com.ldbc.driver.measurements;

public class MetricsExporterException extends Exception
{
    private static final long serialVersionUID = -5243628117143814942L;

    public MetricsExporterException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public MetricsExporterException( String message )
    {
        super( message );
    }

    public MetricsExporterException( Throwable cause )
    {
        super( cause );
    }

    public MetricsExporterException()
    {
        super();
    }

}
