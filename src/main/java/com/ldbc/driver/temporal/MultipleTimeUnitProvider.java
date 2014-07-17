package com.ldbc.driver.temporal;

import java.util.concurrent.TimeUnit;

public interface MultipleTimeUnitProvider<T>
{
    public long asSeconds();

    public long asMilli();

    public long asMicro();

    public long asNano();

    public long as( TimeUnit timeUnit );

    public boolean gt(T other);

    public boolean lt(T other);

    public boolean gte(T other);

    public boolean lte(T other);

    public Duration durationGreaterThan(T other);

    public Duration durationLessThan(T other);

    public T plus( Duration duration );

    public T minus( Duration duration );
}
