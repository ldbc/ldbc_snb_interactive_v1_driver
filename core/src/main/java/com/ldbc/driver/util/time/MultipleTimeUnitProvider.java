package com.ldbc.driver.util.time;

public interface MultipleTimeUnitProvider
{
    public long asSeconds();

    public long asMilli();

    public long asMicro();

    public long asNano();
}
